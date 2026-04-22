"""Connection management for MySQL proxy.

This module manages individual client connections and their lifecycle,
routing queries through the translation pipeline to RDS Data API.
"""

import logging
import socket
import uuid
from dataclasses import dataclass
from typing import Optional

from .config import ConfigurationManager
from .protocol_handler import MySQLProtocolHandler
from .query_translator import QueryTranslator
from .schema_mapper import SchemaMapper
from .rds_client import RDSClient
from .response_translator import ResponseTranslator


logger = logging.getLogger(__name__)


@dataclass
class ConnectionContext:
    """Holds state for a single client connection.
    
    Attributes:
        connection_id: Unique connection identifier
        client_socket: Socket connected to MySQL client
        authenticated: Whether client has completed authentication
        current_database: Currently selected database (if any)
    """
    connection_id: str
    client_socket: socket.socket
    authenticated: bool = False
    current_database: Optional[str] = None


class ConnectionManager:
    """Manages individual client connections and their lifecycle.
    
    Handles connection creation, query routing through the translation
    pipeline, and connection cleanup.
    """
    
    def __init__(self, config: ConfigurationManager):
        """Initialize connection manager.
        
        Args:
            config: Configuration manager instance
        """
        self.config = config
        
        # Create shared components
        self.schema_mapper = SchemaMapper(config.get_schema_mappings())
        self.query_translator = QueryTranslator(self.schema_mapper)
        self.rds_client = RDSClient(config)
        self.response_translator = ResponseTranslator()
        
        # Connection counter for unique IDs
        self._connection_counter = 0
        
        logger.info("Connection manager initialized")
    
    def create_connection(
        self, client_socket: socket.socket
    ) -> ConnectionContext:
        """Create a new connection context.
        
        Args:
            client_socket: Socket connected to MySQL client
            
        Returns:
            New ConnectionContext instance
        """
        # Generate unique connection ID
        self._connection_counter += 1
        connection_id = f"{uuid.uuid4().hex[:8]}-{self._connection_counter}"
        
        context = ConnectionContext(
            connection_id=connection_id,
            client_socket=client_socket,
            authenticated=False,
            current_database=None
        )
        
        logger.info(f"[{connection_id}] Connection created")
        
        return context
    
    def handle_connection(self, context: ConnectionContext) -> None:
        """Main loop for handling a client connection.
        
        Performs handshake, then processes queries until client disconnects.
        
        Args:
            context: Connection context
        """
        connection_id = context.connection_id
        logger.info(f"[{connection_id}] Handling connection")
        
        try:
            # Create protocol handler
            protocol = MySQLProtocolHandler(
                context.client_socket,
                self._connection_counter
            )
            
            # Perform MySQL handshake
            if not protocol.perform_handshake():
                logger.error(f"[{connection_id}] Handshake failed")
                return
            
            context.authenticated = True
            logger.info(f"[{connection_id}] Connection authenticated")
            
            # Main query loop
            while True:
                logger.debug(f"[{connection_id}] Waiting to read next packet from client")
                
                # Read packet from client
                packet = protocol.read_packet()
                
                if packet is None:
                    logger.info(f"[{connection_id}] Client disconnected")
                    break
                
                logger.debug(f"[{connection_id}] Received packet of length {len(packet)}")
                
                # Parse query from packet
                query = protocol.parse_query_packet(packet)
                
                if query is None:
                    # COM_QUIT or unsupported command
                    logger.info(f"[{connection_id}] Received quit or unsupported command, closing connection")
                    break
                
                # Route query for execution
                self._route_query(context, protocol, query)
                
                logger.debug(f"[{connection_id}] Query completed, waiting for next packet")
            
        except Exception as e:
            logger.error(
                f"[{connection_id}] Error handling connection: {e}",
                exc_info=True
            )
            
            # Try to send error to client
            try:
                protocol = MySQLProtocolHandler(
                    context.client_socket,
                    self._connection_counter
                )
                protocol.send_error_packet(
                    1105,  # ER_UNKNOWN_ERROR
                    'HY000',
                    f"Internal error: {str(e)}"
                )
            except:
                pass
        
        finally:
            self.close_connection(context)
    
    def _route_query(
        self,
        context: ConnectionContext,
        protocol: MySQLProtocolHandler,
        query: str
    ) -> None:
        """Route query to translator and RDS client for execution.
        
        Args:
            context: Connection context
            protocol: Protocol handler for sending responses
            query: SQL query to execute
        """
        connection_id = context.connection_id
        
        # Check if this is a USE database command
        if query.startswith("__USE_DATABASE__:"):
            database = query.split(":", 1)[1]
            logger.info(f"[{connection_id}] Setting current database to: {database}")
            context.current_database = database
            # Send OK packet
            protocol.send_ok_packet(0, 0)
            return
        
        logger.info(f"[{connection_id}] Executing query: {query[:100]}...")
        
        try:
            # Translate query
            translated_query = self.query_translator.translate(query)
            logger.debug(
                f"[{connection_id}] Translated query: {translated_query[:100]}..."
            )
            
            # Execute via RDS Data API
            result = self.rds_client.execute_query(
                translated_query,
                database=context.current_database
            )
            
            # Check for errors
            if result.error:
                logger.error(
                    f"[{connection_id}] Query execution failed: {result.error}"
                )
                
                # Translate error to MySQL format
                error_code, sql_state, message = \
                    self.response_translator.translate_error(result.error)
                
                # Send error packet
                protocol.send_error_packet(error_code, sql_state, message)
                return
            
            # Translate and send response
            if result.columns:
                # SELECT query - send result set
                columns, rows = self.response_translator.translate_result_set(result)
                protocol.send_result_set(columns, rows)
                
                logger.info(
                    f"[{connection_id}] Query completed: "
                    f"{len(rows)} rows returned"
                )
            else:
                # DML query - send OK packet with affected rows
                affected_rows = self.response_translator.get_affected_rows(result)
                last_insert_id = self.response_translator.get_last_insert_id(result)
                
                protocol.send_ok_packet(affected_rows, last_insert_id)
                
                logger.info(
                    f"[{connection_id}] Query completed: "
                    f"{affected_rows} rows affected"
                )
        
        except Exception as e:
            logger.error(
                f"[{connection_id}] Error routing query: {e}",
                exc_info=True
            )
            
            # Send error to client
            protocol.send_error_packet(
                1105,  # ER_UNKNOWN_ERROR
                'HY000',
                f"Query execution failed: {str(e)}"
            )
    
    def close_connection(self, context: ConnectionContext) -> None:
        """Clean up connection resources.
        
        Args:
            context: Connection context to clean up
        """
        connection_id = context.connection_id
        logger.info(f"[{connection_id}] Closing connection")
        
        try:
            context.client_socket.close()
        except Exception as e:
            logger.error(
                f"[{connection_id}] Error closing socket: {e}"
            )
        
        logger.info(f"[{connection_id}] Connection closed")
