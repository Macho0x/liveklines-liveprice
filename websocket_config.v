module configs

import time

// WebSocketConfig holds all WebSocket-related configuration parameters
// Based on Binance WebSocket API specifications (2025-01-28)
pub struct WebSocketConfig {
pub:
	// Connection settings
	read_timeout          time.Duration = 30 * time.second  // Conservative timeout for stability
	write_timeout         time.Duration = 15 * time.second  // Conservative write timeout
	connect_timeout       time.Duration = 10 * time.second  // Connection establishment timeout
	
	// Ping/Pong settings (based on Binance specs)
	ping_interval_initial time.Duration = 20 * time.second  // Conservative start (Binance sends ping every 20s)
	ping_interval_normal  time.Duration = 15 * time.second  // Aggressive mode after connection maturity
	ping_interval_circuit time.Duration = 30 * time.second  // Circuit breaker mode
	
	// Circuit breaker settings
	circuit_breaker_threshold_pings int           = 4                // Activate circuit breaker at ping #4
	circuit_breaker_threshold_age    time.Duration = 60 * time.second // Activate after 60 seconds
	proactive_reconnect_pings        int           = 5                // Trigger proactive reconnection at ping #5
	proactive_reconnect_age          time.Duration = 75 * time.second // Trigger after 75 seconds
	
	// Reconnection settings
	max_reconnect_attempts   int           = 5                // Maximum reconnection attempts
	reconnect_base_delay     time.Duration = 3 * time.second  // Base delay for exponential backoff
	reconnect_max_delay      time.Duration = 30 * time.second // Maximum delay between attempts
	reconnect_jitter_factor  f64           = 0.1              // Jitter factor to avoid thundering herd
	
	// Connection lifecycle settings
	connection_maturity_age  time.Duration = 30 * time.second // Age when connection is considered mature
	max_consecutive_failures int           = 2                // Maximum consecutive ping failures before reconnection
	activity_timeout         time.Duration = 90 * time.second // Timeout for detecting inactive connections
	
	// Binance-specific limits (from official documentation)
	max_connections_per_ip   int           = 300              // 300 connections per 5 minutes per IP
	max_streams_per_conn     int           = 1024             // Maximum streams per connection
	max_messages_per_second  int           = 5                // Maximum incoming messages per second
	connection_lifetime      time.Duration = 24 * time.hour  // Binance disconnects after 24 hours
	
	// Stabilization delays
	connection_establish_delay time.Duration = 200 * time.millisecond // Delay after connection establishment
	client_close_delay         time.Duration = 100 * time.millisecond // Delay before closing old clients
	
	// Goroutine management
	max_concurrent_reconnections int = 10 // Maximum concurrent reconnection goroutines
	
	// Debug and monitoring
	enable_ping_logging      bool = true  // Enable detailed ping/pong logging
	enable_activity_tracking bool = true  // Enable connection activity tracking
	enable_circuit_breaker   bool = true  // Enable circuit breaker functionality
}

// WebSocketEndpoints holds WebSocket endpoint configurations
pub struct WebSocketEndpoints {
pub:
	// Production endpoints
	production_base      string = 'wss://stream.binance.com:9443'
	production_alt       string = 'wss://stream.binance.com:443'
	production_data_only string = 'wss://data-stream.binance.vision'
	
	// Testnet endpoints
	testnet_base string = 'wss://testnet.binance.vision'
	
	// Stream paths
	raw_stream_path      string = '/ws'      // For single raw streams: /ws/<streamName>
	combined_stream_path string = '/stream'  // For combined streams: /stream?streams=<streamName1>/<streamName2>
}

// StreamConfig holds configuration for specific stream types
pub struct StreamConfig {
pub:
	// Kline stream settings
	kline_update_speed_1s    time.Duration = 1000 * time.millisecond // 1s klines update every 1000ms
	kline_update_speed_other time.Duration = 2000 * time.millisecond // Other intervals update every 2000ms
	
	// Ticker stream settings
	ticker_update_speed      time.Duration = 1000 * time.millisecond // Ticker updates every 1000ms
	mini_ticker_update_speed time.Duration = 1000 * time.millisecond // Mini ticker updates every 1000ms
	
	// Trade stream settings (real-time)
	trade_update_speed       time.Duration = 0 * time.millisecond    // Real-time updates
	
	// Depth stream settings
	depth_update_speed_normal time.Duration = 1000 * time.millisecond // Normal depth updates every 1000ms
	depth_update_speed_fast   time.Duration = 100 * time.millisecond  // Fast depth updates every 100ms
}

// Default WebSocket configuration optimized for Binance specifications
pub const default_websocket_config = WebSocketConfig{
	read_timeout:          30 * time.second
	write_timeout:         15 * time.second
	connect_timeout:       10 * time.second
	ping_interval_initial: 20 * time.second
	ping_interval_normal:  15 * time.second
	ping_interval_circuit: 30 * time.second
	circuit_breaker_threshold_pings: 4
	circuit_breaker_threshold_age:   60 * time.second
	proactive_reconnect_pings:       5
	proactive_reconnect_age:         75 * time.second
	max_reconnect_attempts:   5
	reconnect_base_delay:     3 * time.second
	reconnect_max_delay:      30 * time.second
	reconnect_jitter_factor:  0.1
	connection_maturity_age:  30 * time.second
	max_consecutive_failures: 2
	activity_timeout:         90 * time.second
	max_connections_per_ip:   300
	max_streams_per_conn:     1024
	max_messages_per_second:  5
	connection_lifetime:      24 * time.hour
	connection_establish_delay: 200 * time.millisecond
	client_close_delay:         100 * time.millisecond
	max_concurrent_reconnections: 10
	enable_ping_logging:      true
	enable_activity_tracking: true
	enable_circuit_breaker:   true
}

// Default WebSocket endpoints
pub const default_websocket_endpoints = WebSocketEndpoints{
	production_base:      'wss://stream.binance.com:9443'
	production_alt:       'wss://stream.binance.com:443'
	production_data_only: 'wss://data-stream.binance.vision'
	testnet_base:         'wss://testnet.binance.vision'
	raw_stream_path:      '/ws'
	combined_stream_path: '/stream'
}

// Default stream configuration
pub const default_stream_config = StreamConfig{
	kline_update_speed_1s:    1000 * time.millisecond
	kline_update_speed_other: 2000 * time.millisecond
	ticker_update_speed:      1000 * time.millisecond
	mini_ticker_update_speed: 1000 * time.millisecond
	trade_update_speed:       0 * time.millisecond
	depth_update_speed_normal: 1000 * time.millisecond
	depth_update_speed_fast:   100 * time.millisecond
}

// load_websocket_config returns the WebSocket configuration
pub fn load_websocket_config() WebSocketConfig {
	return default_websocket_config
}

// load_websocket_endpoints returns the WebSocket endpoints configuration
pub fn load_websocket_endpoints() WebSocketEndpoints {
	return default_websocket_endpoints
}

// load_stream_config returns the stream configuration
pub fn load_stream_config() StreamConfig {
	return default_stream_config
}

// get_websocket_url returns the appropriate WebSocket URL based on mode and endpoint type
pub fn get_websocket_url(live_mode bool, use_data_only bool) string {
	endpoints := load_websocket_endpoints()
	
	if live_mode {
		if use_data_only {
			return endpoints.production_data_only
		}
		return endpoints.production_base
	}
	return endpoints.testnet_base
}

// get_stream_url constructs a complete stream URL
pub fn get_stream_url(base_url string, stream_name string, is_combined bool) string {
	endpoints := load_websocket_endpoints()
	
	if is_combined {
		return '${base_url}${endpoints.combined_stream_path}?streams=${stream_name}'
	}
	return '${base_url}${endpoints.raw_stream_path}/${stream_name}'
}

// validate_websocket_config validates the WebSocket configuration
pub fn validate_websocket_config(config WebSocketConfig) !bool {
	// Validate ping intervals
	if config.ping_interval_normal >= config.ping_interval_initial {
		return error('ping_interval_normal must be less than ping_interval_initial')
	}
	
	// Validate circuit breaker settings
	if config.circuit_breaker_threshold_pings >= config.proactive_reconnect_pings {
		return error('circuit_breaker_threshold_pings must be less than proactive_reconnect_pings')
	}
	
	// Validate timeouts
	if config.write_timeout >= config.read_timeout {
		return error('write_timeout should be less than read_timeout')
	}
	
	// Validate Binance limits
	if config.max_streams_per_conn > 1024 {
		return error('max_streams_per_conn cannot exceed Binance limit of 1024')
	}
	
	if config.max_messages_per_second > 5 {
		return error('max_messages_per_second cannot exceed Binance limit of 5')
	}
	
	return true
}
