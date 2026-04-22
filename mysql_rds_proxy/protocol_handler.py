"""MySQL protocol handler for wire protocol communication.

This module implements a minimal subset of the MySQL wire protocol sufficient
for proxy functionality, including handshake, query packets, and result sets.
"""

import logging
import socket
import struct
import secrets
from typing import Any, Optional

from .response_translator import ColumnDef


logger = logging.getLogger(__name__)


# MySQL protocol constants
PROTOCOL_VERSION = 10
SERVER_VERSION = "8.0.0-mysql-rds-proxy"

# Capability flags (simplified subset)
CLIENT_LONG_PASSWORD = 0x00000001
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_TRANSACTIONS = 0x00002000
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000
CLIENT_DEPRECATE_EOF = 0x01000000  # Client expects OK instead of EOF packets

# Command types
COM_QUIT = 0x01
COM_INIT_DB = 0x02
COM_QUERY = 0x03

# Column flags
NOT_NULL_FLAG = 0x0001
PRI_KEY_FLAG = 0x0002


class MySQLProtocolHandler:
    """Handler for MySQL wire protocol communication.
    
    Implements a minimal subset of MySQL protocol for proxy functionality:
    - Handshake sequence (server greeting, auth response)
    - Packet reading and writing
    - COM_QUERY packet parsing
    - Result set packet formatting
    - OK and error packet formatting
    """
    
    def __init__(self, client_socket: socket.socket, connection_id: int):
        """Initialize protocol handler for a connection.
        
        Args:
            client_socket: Socket connected to MySQL client
            connection_id: Unique connection identifier
        """
        self.socket = client_socket
        self.connection_id = connection_id
        self.sequence_id = 0
        self.authenticated = False
        self.client_capabilities = 0  # Store client capability flags
        
        # Set socket timeout to detect client disconnection
        # Use a reasonable timeout (30 seconds) to allow for slow queries
        try:
            self.socket.settimeout(30.0)
        except Exception as e:
            logger.warning(f"[{connection_id}] Could not set socket timeout: {e}")
        
        logger.debug(f"[{connection_id}] Protocol handler initialized")
    
    def perform_handshake(self) -> bool:
        """Execute MySQL handshake sequence.
        
        Sends server greeting and processes authentication response.
        For simplicity, accepts any credentials (authentication is
        handled by AWS Secrets Manager for actual database access).
        
        Returns:
            True if handshake successful, False otherwise
        """
        logger.info(f"[{self.connection_id}] Starting MySQL handshake")
        
        try:
            # Send server greeting (handshake initialization packet)
            self._send_handshake_packet()
            
            # Read client authentication response
            auth_packet = self._read_packet()
            
            if not auth_packet:
                logger.error(f"[{self.connection_id}] No auth packet received")
                return False
            
            # Parse client capability flags from auth packet
            if len(auth_packet) >= 4:
                self.client_capabilities = struct.unpack('<I', auth_packet[:4])[0]
                logger.info(
                    f"[{self.connection_id}] Client capabilities: 0x{self.client_capabilities:08x}"
                )
                
                # Check if client supports CLIENT_DEPRECATE_EOF
                if self.client_capabilities & CLIENT_DEPRECATE_EOF:
                    logger.info(
                        f"[{self.connection_id}] Client uses CLIENT_DEPRECATE_EOF "
                        "(will send OK packets instead of EOF)"
                    )
            
            # Parse authentication (simplified - we accept any credentials)
            # In a real implementation, we would validate credentials here
            logger.debug(f"[{self.connection_id}] Auth packet received, accepting")
            
            # Send OK packet to complete handshake
            self._send_ok_packet(0, 0)
            
            self.authenticated = True
            logger.info(f"[{self.connection_id}] Handshake completed successfully")
            
            return True
            
        except Exception as e:
            logger.error(
                f"[{self.connection_id}] Handshake failed: {e}",
                exc_info=True
            )
            return False
    
    def _send_handshake_packet(self):
        """Send MySQL server greeting packet."""
        # Generate random auth plugin data (salt)
        auth_plugin_data_part_1 = secrets.token_bytes(8)
        auth_plugin_data_part_2 = secrets.token_bytes(12)
        
        # Build handshake packet
        packet = bytearray()
        
        # Protocol version (1 byte)
        packet.append(PROTOCOL_VERSION)
        
        # Server version string (null-terminated)
        packet.extend(SERVER_VERSION.encode('utf-8'))
        packet.append(0)
        
        # Connection ID (4 bytes)
        packet.extend(struct.pack('<I', self.connection_id))
        
        # Auth plugin data part 1 (8 bytes)
        packet.extend(auth_plugin_data_part_1)
        
        # Filler (1 byte)
        packet.append(0)
        
        # Capability flags lower 2 bytes
        capabilities = (
            CLIENT_LONG_PASSWORD |
            CLIENT_PROTOCOL_41 |
            CLIENT_TRANSACTIONS |
            CLIENT_SECURE_CONNECTION |
            CLIENT_PLUGIN_AUTH
            # Note: NOT advertising CLIENT_DEPRECATE_EOF - we'll use traditional EOF packets
        )
        packet.extend(struct.pack('<H', capabilities & 0xFFFF))
        
        # Character set (1 byte) - utf8_general_ci
        packet.append(33)
        
        # Status flags (2 bytes)
        packet.extend(struct.pack('<H', 0x0002))  # SERVER_STATUS_AUTOCOMMIT
        
        # Capability flags upper 2 bytes
        packet.extend(struct.pack('<H', (capabilities >> 16) & 0xFFFF))
        
        # Auth plugin data length (1 byte)
        packet.append(21)
        
        # Reserved (10 bytes)
        packet.extend(bytes(10))
        
        # Auth plugin data part 2 (12 bytes + null terminator)
        packet.extend(auth_plugin_data_part_2)
        packet.append(0)
        
        # Auth plugin name (null-terminated)
        packet.extend(b'mysql_native_password\x00')
        
        self._write_packet(bytes(packet))
    
    def read_packet(self) -> Optional[bytes]:
        """Read a MySQL packet from the client.
        
        Returns:
            Packet payload (without header) or None if connection closed
        """
        try:
            # Read packet header (4 bytes: 3 for length, 1 for sequence)
            header = self._recv_exactly(4)
            if not header:
                logger.debug(f"[{self.connection_id}] No header received, connection closed")
                return None
            
            # Parse header
            payload_length = struct.unpack('<I', header[:3] + b'\x00')[0]
            sequence_id = header[3]
            
            logger.debug(
                f"[{self.connection_id}] Reading packet: "
                f"length={payload_length}, seq={sequence_id}, expected_seq={self.sequence_id}"
            )
            
            # Read payload
            payload = self._recv_exactly(payload_length)
            if not payload:
                logger.debug(f"[{self.connection_id}] No payload received, connection closed")
                return None
            
            # Log first byte to see command type
            if len(payload) > 0:
                logger.debug(f"[{self.connection_id}] Packet command byte: 0x{payload[0]:02x}")
            
            self.sequence_id = (sequence_id + 1) % 256
            
            return payload
            
        except Exception as e:
            logger.error(
                f"[{self.connection_id}] Error reading packet: {e}",
                exc_info=True
            )
            return None
    
    def _read_packet(self) -> Optional[bytes]:
        """Internal packet reading (alias for read_packet)."""
        return self.read_packet()
    
    def parse_query_packet(self, packet: bytes) -> Optional[str]:
        """Extract SQL query from COM_QUERY packet.
        
        Args:
            packet: MySQL packet payload
            
        Returns:
            SQL query string or None if not a query packet
        """
        if not packet or len(packet) < 1:
            return None
        
        command = packet[0]
        
        if command == COM_QUIT:
            logger.info(f"[{self.connection_id}] Received COM_QUIT")
            return None
        
        if command == COM_INIT_DB:
            # USE database command
            database = packet[1:].decode('utf-8', errors='replace')
            logger.info(f"[{self.connection_id}] Received COM_INIT_DB: {database}")
            # Return a special marker that the connection manager can handle
            return f"__USE_DATABASE__:{database}"
        
        if command == COM_QUERY:
            # Query text follows command byte
            query = packet[1:].decode('utf-8', errors='replace')
            logger.debug(
                f"[{self.connection_id}] Parsed query: {query[:100]}..."
            )
            return query
        
        logger.warning(
            f"[{self.connection_id}] Unsupported command: {command}"
        )
        return None
    
    def send_result_set(
        self, columns: list[ColumnDef], rows: list[list[Any]]
    ) -> None:
        """Send a result set to the client.
        
        Sends column definitions followed by row data in MySQL protocol format.
        Uses traditional EOF packets (we don't advertise CLIENT_DEPRECATE_EOF).
        
        Args:
            columns: List of column definitions
            rows: List of rows (each row is a list of values)
        """
        logger.debug(
            f"[{self.connection_id}] Sending result set: "
            f"{len(columns)} columns, {len(rows)} rows"
        )
        
        try:
            # Reset sequence for new result set
            self.sequence_id = 0
            logger.debug(f"[{self.connection_id}] Reset sequence_id to 0")
            
            # Send column count packet
            self._write_packet(self._encode_length(len(columns)))
            logger.debug(f"[{self.connection_id}] Sent column count: {len(columns)}")
            
            # Send column definition packets
            for i, col in enumerate(columns):
                self._send_column_definition(col)
                logger.debug(f"[{self.connection_id}] Sent column definition {i+1}/{len(columns)}: {col.name}")
            
            # Send EOF packet after column definitions
            self._send_eof_packet()
            logger.debug(f"[{self.connection_id}] Sent EOF after column definitions")
            
            # Send row data packets
            for i, row in enumerate(rows):
                self._send_row_data(row)
                if i < 5 or i >= len(rows) - 2:  # Log first 5 and last 2 rows
                    logger.debug(f"[{self.connection_id}] Sent row {i+1}/{len(rows)}")
            
            # Send EOF packet after rows
            self._send_eof_packet()
            logger.debug(f"[{self.connection_id}] Sent EOF after rows, final sequence_id={self.sequence_id}")
            
            # Reset sequence_id for next command from client
            self.sequence_id = 0
            logger.debug(f"[{self.connection_id}] Reset sequence_id to 0 for next command")
            
            logger.debug(f"[{self.connection_id}] Result set sent successfully")
            
        except Exception as e:
            logger.error(
                f"[{self.connection_id}] Error sending result set: {e}",
                exc_info=True
            )
            raise
    
    def _send_column_definition(self, col: ColumnDef):
        """Send a column definition packet.
        
        Args:
            col: Column definition
        """
        packet = bytearray()
        
        # Catalog (always "def")
        packet.extend(self._encode_length_encoded_string(b'def'))
        
        # Schema (empty)
        packet.extend(self._encode_length_encoded_string(b''))
        
        # Table (empty)
        packet.extend(self._encode_length_encoded_string(b''))
        
        # Original table (empty)
        packet.extend(self._encode_length_encoded_string(b''))
        
        # Name
        packet.extend(self._encode_length_encoded_string(col.name.encode('utf-8')))
        
        # Original name
        packet.extend(self._encode_length_encoded_string(col.name.encode('utf-8')))
        
        # Length of fixed-length fields (always 0x0c)
        packet.append(0x0c)
        
        # Character set (utf8_general_ci = 33)
        packet.extend(struct.pack('<H', 33))
        
        # Column length
        packet.extend(struct.pack('<I', col.max_length))
        
        # Type
        packet.append(col.type_code)
        
        # Flags
        packet.extend(struct.pack('<H', col.flags))
        
        # Decimals
        packet.append(col.decimals)
        
        # Filler (2 bytes)
        packet.extend(b'\x00\x00')
        
        self._write_packet(bytes(packet))
    
    def _send_row_data(self, row: list[Any]):
        """Send a row data packet.
        
        Args:
            row: List of column values
        """
        packet = bytearray()
        
        for value in row:
            if value is None:
                # NULL value (0xfb)
                packet.append(0xfb)
            else:
                # Convert value to string and encode
                value_str = str(value).encode('utf-8')
                packet.extend(self._encode_length_encoded_string(value_str))
        
        self._write_packet(bytes(packet))
    
    def send_ok_packet(
        self, affected_rows: int, last_insert_id: int = 0
    ) -> None:
        """Send an OK packet for successful non-SELECT queries.
        
        Args:
            affected_rows: Number of rows affected
            last_insert_id: Last insert ID (for INSERT statements)
        """
        logger.debug(
            f"[{self.connection_id}] Sending OK packet: "
            f"affected_rows={affected_rows}, last_insert_id={last_insert_id}"
        )
        
        self._send_ok_packet(affected_rows, last_insert_id)
    
    def _send_ok_packet(self, affected_rows: int, last_insert_id: int):
        """Internal OK packet sending."""
        packet = bytearray()
        
        # OK header (0x00)
        packet.append(0x00)
        
        # Affected rows (length-encoded integer)
        packet.extend(self._encode_length(affected_rows))
        
        # Last insert ID (length-encoded integer)
        packet.extend(self._encode_length(last_insert_id))
        
        # Status flags (2 bytes) - SERVER_STATUS_AUTOCOMMIT
        packet.extend(struct.pack('<H', 0x0002))
        
        # Warnings (2 bytes)
        packet.extend(struct.pack('<H', 0))
        
        self._write_packet(bytes(packet))
        
        # Reset sequence_id for next command from client
        self.sequence_id = 0
    
    def send_error_packet(
        self, error_code: int, sql_state: str, message: str
    ) -> None:
        """Send an error packet to the client.
        
        Args:
            error_code: MySQL error code
            sql_state: SQL state (5 characters)
            message: Error message
        """
        logger.debug(
            f"[{self.connection_id}] Sending error packet: "
            f"code={error_code}, state={sql_state}, message={message}"
        )
        
        packet = bytearray()
        
        # Error header (0xff)
        packet.append(0xff)
        
        # Error code (2 bytes)
        packet.extend(struct.pack('<H', error_code))
        
        # SQL state marker (#)
        packet.append(ord('#'))
        
        # SQL state (5 bytes)
        sql_state_bytes = sql_state.encode('utf-8')[:5].ljust(5, b' ')
        packet.extend(sql_state_bytes)
        
        # Error message
        packet.extend(message.encode('utf-8'))
        
        self._write_packet(bytes(packet))
        
        # Reset sequence_id for next command from client
        self.sequence_id = 0
    
    def _send_eof_packet(self):
        """Send an EOF packet."""
        packet = bytearray()
        
        # EOF header (0xfe)
        packet.append(0xfe)
        
        # Warnings (2 bytes)
        packet.extend(struct.pack('<H', 0))
        
        # Status flags (2 bytes) - SERVER_STATUS_AUTOCOMMIT
        packet.extend(struct.pack('<H', 0x0002))
        
        self._write_packet(bytes(packet))
    
    def _send_ok_packet_for_resultset(self):
        """Send an OK packet for result set (when CLIENT_DEPRECATE_EOF is set).
        
        When CLIENT_DEPRECATE_EOF is set, we send an OK-like packet using 0xfe header.
        The client distinguishes it from EOF by packet length:
        - EOF packet: exactly 5 bytes (0xfe + 2 bytes warnings + 2 bytes status)
        - OK packet: 7+ bytes (0xfe + length-encoded values + status + warnings)
        """
        packet = bytearray()
        
        # Header byte (0xfe)
        packet.append(0xfe)
        
        # Affected rows (length-encoded integer, 0 for SELECT)
        packet.extend(self._encode_length(0))
        
        # Last insert ID (length-encoded integer, 0 for SELECT)
        packet.extend(self._encode_length(0))
        
        # Status flags (2 bytes) - SERVER_STATUS_AUTOCOMMIT
        packet.extend(struct.pack('<H', 0x0002))
        
        # Warnings (2 bytes)
        packet.extend(struct.pack('<H', 0))
        
        self._write_packet(bytes(packet))
    
    def _write_packet(self, payload: bytes):
        """Write a MySQL packet to the client.
        
        Args:
            payload: Packet payload (without header)
        """
        # Build packet header
        header = bytearray()
        
        # Payload length (3 bytes, little-endian)
        length = len(payload)
        header.extend(struct.pack('<I', length)[:3])
        
        # Sequence ID (1 byte)
        header.append(self.sequence_id)
        
        logger.debug(
            f"[{self.connection_id}] Writing packet: "
            f"length={length}, seq={self.sequence_id}"
        )
        
        # Send header + payload
        try:
            data = header + payload
            self.socket.sendall(data)
            # Ensure data is sent immediately (disable Nagle's algorithm effect)
            self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            logger.debug(f"[{self.connection_id}] Packet sent successfully")
        except Exception as e:
            logger.error(
                f"[{self.connection_id}] Error writing packet: {e}"
            )
            raise
        
        self.sequence_id = (self.sequence_id + 1) % 256
    
    def _recv_exactly(self, n: int) -> Optional[bytes]:
        """Receive exactly n bytes from socket.
        
        Args:
            n: Number of bytes to receive
            
        Returns:
            Bytes received or None if connection closed
        """
        data = bytearray()
        while len(data) < n:
            try:
                chunk = self.socket.recv(n - len(data))
                if not chunk:
                    return None
                data.extend(chunk)
            except socket.timeout:
                logger.debug(
                    f"[{self.connection_id}] Socket timeout while reading"
                )
                return None
            except socket.error as e:
                logger.error(
                    f"[{self.connection_id}] Socket error in _recv_exactly: {e}"
                )
                return None
        return bytes(data)
    
    def _encode_length(self, value: int) -> bytes:
        """Encode integer as length-encoded integer.
        
        Args:
            value: Integer value to encode
            
        Returns:
            Encoded bytes
        """
        if value < 251:
            return bytes([value])
        elif value < 2**16:
            return b'\xfc' + struct.pack('<H', value)
        elif value < 2**24:
            return b'\xfd' + struct.pack('<I', value)[:3]
        else:
            return b'\xfe' + struct.pack('<Q', value)
    
    def _encode_length_encoded_string(self, data: bytes) -> bytes:
        """Encode string as length-encoded string.
        
        Args:
            data: String data as bytes
            
        Returns:
            Encoded bytes (length + data)
        """
        return self._encode_length(len(data)) + data
    
    def close(self):
        """Close the connection."""
        try:
            self.socket.close()
            logger.debug(f"[{self.connection_id}] Connection closed")
        except Exception as e:
            logger.error(
                f"[{self.connection_id}] Error closing connection: {e}"
            )
