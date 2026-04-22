"""Proxy server for MySQL-to-RDS Data API translation.

This module provides the main server that accepts MySQL connections
and routes them to AWS RDS Data API using mysql-mimic for protocol handling.
"""

import asyncio
import logging
import signal

from mysql_mimic import MysqlServer

from .config import ConfigurationManager
from .session import RDSProxySession


logger = logging.getLogger(__name__)


class ProxyServer:
    """Main proxy server using mysql-mimic for MySQL protocol handling.
    
    This server uses the mysql-mimic library to handle all MySQL wire protocol
    details, allowing us to focus on query translation and RDS API integration.
    """
    
    def __init__(self, config: ConfigurationManager):
        """Initialize proxy server with configuration.
        
        Args:
            config: Configuration manager instance
        """
        self.config = config
        
        # Initialize shared session components
        RDSProxySession.initialize(config)
        
        # Create mysql-mimic server
        self.server = MysqlServer(
            host=config.get_listen_host(),
            port=config.get_listen_port(),
            session_factory=RDSProxySession
        )
        
        logger.info("Proxy server initialized with mysql-mimic")
    
    def start(self) -> None:
        """Start the proxy server.
        
        This method starts the mysql-mimic server and runs the asyncio event loop.
        It handles graceful shutdown on SIGINT/SIGTERM.
        """
        logger.info(
            f"Starting proxy server on {self.config.get_listen_host()}:"
            f"{self.config.get_listen_port()}"
        )
        
        print(f"MySQL-to-RDS Data API Proxy (using mysql-mimic)")
        print(f"Listening on {self.config.get_listen_host()}:{self.config.get_listen_port()}")
        print(f"Press Ctrl+C to stop")
        print()
        
        # Set up signal handlers for graceful shutdown
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        def signal_handler():
            logger.info("Received shutdown signal, stopping server...")
            loop.stop()
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
        
        try:
            # Run the server
            loop.run_until_complete(self.server.serve_forever())
        except KeyboardInterrupt:
            logger.info("Server interrupted by user")
        finally:
            logger.info("Server stopped")
            print("\nProxy server stopped")
            loop.close()
    
    def stop(self) -> None:
        """Stop the proxy server.
        
        This method is called to gracefully shut down the server.
        """
        logger.info("Stopping proxy server")
        # mysql-mimic handles cleanup automatically
