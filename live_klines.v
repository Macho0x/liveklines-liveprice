module connections

import net.websocket
import time
import json
import sync
import configs
import utils
import database
import log

// BinanceKlineMessage represents Binance kline/candlestick stream message
pub struct BinanceKlineMessage {
pub:
	stream string           @[json: 'stream']
	data   BinanceKlineData @[json: 'data']
}

pub struct BinanceKlineData {
pub:
	event_type string       @[json: 'e'] // Event type (kline)
	event_time i64          @[json: 'E'] // Event time
	symbol     string       @[json: 's'] // Symbol
	kline      BinanceKline @[json: 'k'] // Kline data
}

pub struct BinanceKline {
pub:
	kline_start_time             i64    @[json: 't'] // Kline start time
	kline_close_time             i64    @[json: 'T'] // Kline close time
	symbol                       string @[json: 's'] // Symbol
	interval                     string @[json: 'i'] // Interval
	first_trade_id               i64    @[json: 'f'] // First trade ID
	last_trade_id                i64    @[json: 'L'] // Last trade ID
	open_price                   string @[json: 'o'] // Open price
	close_price                  string @[json: 'c'] // Close price
	high_price                   string @[json: 'h'] // High price
	low_price                    string @[json: 'l'] // Low price
	base_asset_volume            string @[json: 'v'] // Base asset volume
	number_of_trades             i64    @[json: 'n'] // Number of trades
	is_kline_closed              bool   @[json: 'x'] // Is this kline closed?
	quote_asset_volume           string @[json: 'q'] // Quote asset volume
	taker_buy_base_asset_volume  string @[json: 'V'] // Taker buy base asset volume
	taker_buy_quote_asset_volume string @[json: 'Q'] // Taker buy quote asset volume
	ignore                       string @[json: 'B'] // Ignore
}

// UUID-compatible RollingKlinesManager interface
pub interface IUUIDRollingKlinesManager {
mut:
	add_new_kline(symbol string, timeframe string, kline configs.Kline) !
	update_kline_if_exists(symbol string, timeframe string, kline configs.Kline) !
}

// LiveKlineManager manages real-time kline WebSocket connections (UUID system)
@[heap]
pub struct LiveKlineManager {
pub mut:
	clients                []websocket.Client
	client_urls            []string // track URLs for each client index
	uuid_trading_manager   &database.UUIDTradingManager
	rolling_klines_manager IUUIDRollingKlinesManager
	is_running             bool
	is_testnet_mode        bool
	kline_handlers         map[string]fn (configs.Kline, string) // symbol -> handler function
	reconnect_attempts     map[string]int                        // track reconnection attempts per client
	reconnecting_streams   map[string]bool                       // prevent duplicate reconnections
	max_reconnect_attempts int = 5
	reconnect_delay_ms     int = 5000 // 5 seconds initial delay
	mutex                  sync.Mutex                 // protect client operations
	reconnection_manager   &utils.ReconnectionManager // manage reconnection goroutines
}

// new_live_kline_manager creates a new live kline manager with UUID support
pub fn new_live_kline_manager(mut uuid_trading_manager database.UUIDTradingManager, mut rolling_klines_manager IUUIDRollingKlinesManager) LiveKlineManager {
	return LiveKlineManager{
		clients:                []websocket.Client{}
		client_urls:            []string{}
		uuid_trading_manager:   &uuid_trading_manager
		rolling_klines_manager: rolling_klines_manager
		is_running:             false
		is_testnet_mode:        false
		kline_handlers:         map[string]fn (configs.Kline, string){}
		reconnect_attempts:     map[string]int{}
		reconnecting_streams:   map[string]bool{}
		max_reconnect_attempts: 5
		reconnect_delay_ms:     5000
		mutex:                  sync.new_mutex()
		reconnection_manager:   utils.new_reconnection_manager(10) // Max 10 concurrent reconnections
	}
}

// connect_kline_streams establishes WebSocket connections for kline streams
pub fn (mut lkm LiveKlineManager) connect_kline_streams(is_testnet_mode bool) {
	lkm.is_testnet_mode = is_testnet_mode
	connection_conf := configs.load_connection_config(is_testnet_mode)
	base_ws_url := connection_conf.urls.websocket
	connect_cid := 'live_klines_connect'

	// Conservative client options for better stability
	client_opt := websocket.ClientOpt{
		read_timeout:  30 * time.second  // Longer timeout for stability
		write_timeout: 15 * time.second  // Conservative write timeout
		logger:        &log.Log{}
	}

	trading_symbols := configs.get_enabled_trading_symbols()
	data_symbols := configs.get_enabled_data_symbols()

	utils.info(connect_cid, 'Connecting to kline streams: ${trading_symbols.len} trading + ${data_symbols.len} data (UUID system)',
		@FILE, @LINE.int())

	// Connect trading symbol kline streams
	for symbol_config in trading_symbols {
		stream_path := '${symbol_config.symbol.to_lower()}@kline_${symbol_config.timeframe.to_api_string()}'
		full_ws_url := '${base_ws_url}/${stream_path}'
		utils.info(connect_cid, 'Attempting to connect to trading kline stream: ${full_ws_url}',
			@FILE, @LINE.int())

		mut client := websocket.new_client(full_ws_url, client_opt) or {
			utils.error(connect_cid, 'Failed to create kline client for ${full_ws_url}: ${err}',
				@FILE, @LINE.int())
			continue
		}

		unsafe {
			client.on_open(lkm.on_kline_open)
			client.on_message(lkm.on_kline_message)
			client.on_error(lkm.on_kline_error)
			client.on_close(lkm.on_kline_close)
		}
		client.connect() or {
			utils.error(connect_cid, 'Failed to connect to ${full_ws_url}: ${err}', @FILE,
				@LINE.int())
			continue
		}

		lkm.mutex.@lock()
		lkm.clients << client
		lkm.client_urls << full_ws_url
		lkm.mutex.unlock()
		utils.info(connect_cid, 'Successfully initiated kline connection to ${full_ws_url}',
			@FILE, @LINE.int())
	}

	// Connect data symbol kline streams
	for symbol_config in data_symbols {
		stream_path := '${symbol_config.symbol.to_lower()}@kline_${symbol_config.timeframe.to_api_string()}'
		full_ws_url := '${base_ws_url}/${stream_path}'
		utils.info(connect_cid, 'Attempting to connect to data kline stream: ${full_ws_url}',
			@FILE, @LINE.int())

		mut client := websocket.new_client(full_ws_url, client_opt) or {
			utils.error(connect_cid, 'Failed to create kline client for ${full_ws_url}: ${err}',
				@FILE, @LINE.int())
			continue
		}

		unsafe {
			client.on_open(lkm.on_kline_open)
			client.on_message(lkm.on_kline_message)
			client.on_error(lkm.on_kline_error)
			client.on_close(lkm.on_kline_close)
		}
		client.connect() or {
			utils.error(connect_cid, 'Failed to connect to ${full_ws_url}: ${err}', @FILE,
				@LINE.int())
			continue
		}

		lkm.mutex.@lock()
		lkm.clients << client
		lkm.client_urls << full_ws_url
		lkm.mutex.unlock()
		utils.info(connect_cid, 'Successfully initiated kline connection to ${full_ws_url}',
			@FILE, @LINE.int())
	}

	lkm.is_running = true
	utils.info(connect_cid, 'All ${lkm.clients.len} kline stream connections initiated with UUID system',
		@FILE, @LINE.int())
}

// disconnect_all gracefully disconnects all WebSocket connections and cleans up resources
pub fn (mut lkm LiveKlineManager) disconnect_all() {
	cid := 'live_klines_disconnect_all'
	utils.info(cid, 'Disconnecting all kline stream connections...', @FILE, @LINE.int())

	lkm.mutex.@lock()
	defer { lkm.mutex.unlock() }

	lkm.is_running = false

	// Close all WebSocket connections
	for mut client in lkm.clients {
		client.close(1000, 'Normal closure') or {
			utils.warn(cid, 'Failed to close WebSocket connection: ${err}', @FILE, @LINE.int())
		}
	}

	// Clear all tracking data
	lkm.clients.clear()
	lkm.client_urls.clear()
	lkm.reconnect_attempts.clear()
	lkm.reconnecting_streams.clear()
	lkm.kline_handlers.clear()

	// Shutdown reconnection manager
	lkm.reconnection_manager.shutdown(5)

	utils.info(cid, 'All kline stream connections disconnected and resources cleaned up',
		@FILE, @LINE.int())
}

// register_kline_handler registers a custom handler for kline data from a specific symbol
pub fn (mut lkm LiveKlineManager) register_kline_handler(symbol string, handler fn (configs.Kline, string)) {
	lkm.kline_handlers[symbol] = handler
	utils.info('live_klines_register', 'Registered custom kline handler for ${symbol}',
		@FILE, @LINE.int())
}

// WebSocket event handlers for kline streams
fn (mut lkm LiveKlineManager) on_kline_open(mut client websocket.Client) ! {
	cid := 'kline_ws_open:${client.uri.str()}'
	utils.info(cid, 'Kline stream connection opened', @FILE, @LINE.int())

	// Send initial ping to confirm connection
	client.ping() or {
		utils.error(cid, 'Failed to send initial ping: ${err}', @FILE, @LINE.int())
		return err
	}

	// Add circuit breaker heartbeat with exponential backoff
	spawn fn [mut client, cid, mut lkm] () {
		mut ping_count := 0
		mut consecutive_failures := 0
		mut last_activity := time.now()
		mut connection_start_time := time.now()
		mut circuit_breaker_active := false

		// Shorter initial delay to start pinging quickly
		initial_delay := 5 + (client.uri.str().len % 5) // 5-9 seconds based on URL
		time.sleep(initial_delay * time.second)

		for {
			// Check if manager is still running before attempting ping
			if !lkm.is_running {
				utils.debug(cid, 'Manager stopped, ending heartbeat for ${client.uri.str()}', @FILE, @LINE.int())
				break
			}

			// Circuit breaker: if connection is very young, use longer intervals to avoid overwhelming Binance
			connection_age := time.now() - connection_start_time
			ping_interval := if connection_age < 30 * time.second {
				20 * time.second // Conservative interval for new connections
			} else if circuit_breaker_active {
				30 * time.second // Longer interval when circuit breaker is active
			} else {
				15 * time.second // Normal aggressive interval
			}

			time.sleep(ping_interval)
			ping_count++

			// Check for connection inactivity (no messages received)
			time_since_activity := time.now() - last_activity
			if time_since_activity > 90 * time.second {
				utils.warn(cid, 'No activity for ${time_since_activity.seconds()}s, connection may be dead', @FILE, @LINE.int())
				consecutive_failures++
			}

			// Proactive reconnection: if we're approaching known failure point, trigger preemptive reconnection
			if ping_count >= 5 && connection_age > 75 * time.second {
				utils.warn(cid, 'Approaching connection failure point (ping #${ping_count}, age ${connection_age.seconds()}s), triggering proactive reconnection', @FILE, @LINE.int())

				// Trigger proactive reconnection
				if lkm.is_running {
					url := client.uri.str()
					spawn fn [mut lkm, url] () {
						time.sleep(2 * time.second) // Small delay to avoid race conditions
						lkm.attempt_reconnection_safe(url)
					}()
				}
				break
			}

			// Activate circuit breaker if we're approaching known failure point
			if ping_count >= 4 && connection_age > 60 * time.second {
				circuit_breaker_active = true
				utils.debug(cid, 'Circuit breaker activated for ${client.uri.str()} at ping #${ping_count}', @FILE, @LINE.int())
			}

			// Check if we've had too many consecutive failures
			if consecutive_failures >= 2 {
				utils.error(cid, 'Too many consecutive ping failures (${consecutive_failures}), triggering reconnection', @FILE, @LINE.int())
				break
			}

			client.ping() or {
				consecutive_failures++
				utils.error(cid, 'Failed to send ping #${ping_count}: ${err}', @FILE,
					@LINE.int())
				utils.error(cid, 'Connection appears dead (${consecutive_failures} consecutive failures), triggering reconnection',
					@FILE, @LINE.int())

				// Trigger reconnection using existing infrastructure with backoff
				if lkm.is_running {
					url := client.uri.str()

					// Add exponential backoff for reconnection attempts
					backoff_delay := if consecutive_failures == 1 {
						2 * time.second
					} else {
						5 * time.second
					}

					utils.debug(cid, 'Waiting ${backoff_delay / time.second}s before reconnection attempt', @FILE, @LINE.int())
					time.sleep(backoff_delay)

					if !lkm.reconnection_manager.attempt_reconnection(url, fn [mut lkm] (url string) {
						lkm.attempt_reconnection_safe(url)
					}) {
						utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
							@FILE, @LINE.int())
						// Get reconnection manager stats for debugging
						active_reconnections, active_goroutines, available_slots := lkm.reconnection_manager.get_stats()
						utils.debug(cid, 'ReconnectionManager stats: active_reconnections=${active_reconnections}, active_goroutines=${active_goroutines}, available_slots=${available_slots}',
							@FILE, @LINE.int())
					}
				}
				break
			}

			// Reset failure counter on successful ping and update activity time
			consecutive_failures = 0
			last_activity = time.now()
			circuit_breaker_active = false // Reset circuit breaker on successful ping
			utils.debug(cid, 'Sent heartbeat ping #${ping_count} (${ping_interval / time.second}s interval)', @FILE, @LINE.int())
		}
	}()
}

fn (mut lkm LiveKlineManager) on_kline_message(mut client websocket.Client, msg &websocket.Message) ! {
	cid := 'kline_ws_message:${client.uri.str()}'

	// Log message activity for connection health monitoring
	utils.debug(cid, 'Received message: ${msg.payload.len} bytes, opcode: ${int(msg.opcode)}', @FILE, @LINE.int())

	// Handle control frames (ping/pong) explicitly
	if msg.opcode == .ping {
		utils.debug(cid, 'Received PING from server, responding with PONG', @FILE, @LINE.int())
		client.pong() or {
			utils.error(cid, 'Failed to send PONG response: ${err}', @FILE, @LINE.int())
		}
		return
	} else if msg.opcode == .pong {
		utils.debug(cid, 'Received PONG from server', @FILE, @LINE.int())
		return
	}

	// Add debug logging to confirm data message reception
	utils.info(cid, 'Received websocket data message: ${msg.payload.len} bytes', @FILE, @LINE.int())

	// Log raw message for debugging (first 200 chars)
	raw_msg := msg.payload.bytestr()
	preview := if raw_msg.len > 200 { raw_msg[..200] + '...' } else { raw_msg }
	utils.debug(cid, 'Message preview: ${preview}', @FILE, @LINE.int())

	// Parse Binance kline message
	kline_msg := json.decode(BinanceKlineMessage, msg.payload.bytestr()) or {
		utils.error(cid, 'Failed to parse kline message: ${err}', @FILE, @LINE.int())
		return
	}

	// Convert to configs.Kline format
	kline := configs.Kline{
		open_time:                    kline_msg.data.kline.kline_start_time
		open:                         kline_msg.data.kline.open_price
		high:                         kline_msg.data.kline.high_price
		low:                          kline_msg.data.kline.low_price
		close:                        kline_msg.data.kline.close_price
		volume:                       kline_msg.data.kline.base_asset_volume
		close_time:                   kline_msg.data.kline.kline_close_time
		quote_asset_volume:           kline_msg.data.kline.quote_asset_volume
		number_of_trades:             kline_msg.data.kline.number_of_trades
		taker_buy_base_asset_volume:  kline_msg.data.kline.taker_buy_base_asset_volume
		taker_buy_quote_asset_volume: kline_msg.data.kline.taker_buy_quote_asset_volume
		ignore:                       kline_msg.data.kline.ignore
	}

	symbol := kline_msg.data.symbol
	timeframe := kline_msg.data.kline.interval

	// Handle kline updates in rolling window using UUID system
	if kline_msg.data.kline.is_kline_closed {
		// Add closed kline to rolling window
		lkm.rolling_klines_manager.add_new_kline(symbol, timeframe, kline) or {
			utils.error(cid, 'Failed to add closed kline to UUID rolling window: ${err}',
				@FILE, @LINE.int())
		}
		utils.debug(cid, 'Added closed kline to UUID rolling window: ${symbol} ${timeframe} - Close: ${kline.close}',
			@FILE, @LINE.int())
	} else {
		// Update existing kline in rolling window for live updates
		lkm.rolling_klines_manager.update_kline_if_exists(symbol, timeframe, kline) or {
			utils.debug(cid, 'Could not update kline in UUID rolling window (may not exist yet): ${err}',
				@FILE, @LINE.int())
		}
		utils.debug(cid, 'Updated live kline in UUID rolling window: ${symbol} ${timeframe} - Close: ${kline.close}',
			@FILE, @LINE.int())
	}

	// Call custom handler if registered for this symbol
	if handler := lkm.kline_handlers[symbol] {
		handler(kline, timeframe)
	}

	utils.debug(cid, 'Processed kline via UUID system: ${symbol} ${timeframe} - ${if kline_msg.data.kline.is_kline_closed {
		'CLOSED'
	} else {
		'LIVE'
	}}', @FILE, @LINE.int())
}


fn (mut lkm LiveKlineManager) on_kline_error(mut client websocket.Client, err string) ! {
	cid := 'kline_ws_error:${client.uri.str()}'
	utils.error(cid, 'Kline stream error: ${err}', @FILE, @LINE.int())

	// Attempt reconnection on error using managed reconnection
	if lkm.is_running {
		url := client.uri.str()
		if !lkm.reconnection_manager.attempt_reconnection(url, fn [mut lkm] (url string) {
			lkm.attempt_reconnection_safe(url)
		}) {
			utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
				@FILE, @LINE.int())
		}
	}
}

fn (mut lkm LiveKlineManager) on_kline_close(mut client websocket.Client, code int, reason string) ! {
	cid := 'kline_ws_close:${client.uri.str()}'
	utils.info(cid, 'Kline stream connection closed. Code: ${code}, Reason: ${reason}',
		@FILE, @LINE.int())

	// Only attempt reconnection for unexpected closures (not normal closure or reconnection)
	if lkm.is_running && code != 1000 && reason != 'Reconnection' {
		url := client.uri.str()
		utils.debug(cid, 'Unexpected closure, attempting reconnection for ${url}', @FILE, @LINE.int())
		if !lkm.reconnection_manager.attempt_reconnection(url, fn [mut lkm] (url string) {
			lkm.attempt_reconnection_safe(url)
		}) {
			utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
				@FILE, @LINE.int())
		}
	} else {
		utils.debug(cid, 'Normal or intentional closure, no reconnection needed', @FILE, @LINE.int())
	}
}

// attempt_reconnection_safe tries to reconnect a failed WebSocket connection with duplicate prevention
fn (mut lkm LiveKlineManager) attempt_reconnection_safe(original_url string) {
	cid := 'kline_ws_reconnect'

	// Extract stream identifier from URL for tracking attempts
	url_parts := original_url.split('/')
	if url_parts.len == 0 {
		utils.error(cid, 'Cannot parse URL for reconnection: ${original_url}', @FILE,
			@LINE.int())
		return
	}

	stream_identifier := url_parts[url_parts.len - 1] // e.g., "btcusdt@kline_3m"

	// Prevent duplicate reconnection attempts
	lkm.mutex.@lock()
	if stream_identifier in lkm.reconnecting_streams && lkm.reconnecting_streams[stream_identifier] {
		lkm.mutex.unlock()
		utils.debug(cid, 'Reconnection already in progress for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}
	lkm.reconnecting_streams[stream_identifier] = true
	lkm.mutex.unlock()

	defer {
		lkm.mutex.@lock()
		lkm.reconnecting_streams[stream_identifier] = false
		lkm.mutex.unlock()
	}

	// Check reconnection attempts (with mutex protection)
	lkm.mutex.@lock()
	current_attempts := lkm.reconnect_attempts[stream_identifier] or { 0 }
	if current_attempts >= lkm.max_reconnect_attempts {
		lkm.mutex.unlock()
		utils.error(cid, 'Max reconnection attempts (${lkm.max_reconnect_attempts}) reached for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}

	// Increment attempt counter
	lkm.reconnect_attempts[stream_identifier] = current_attempts + 1
	lkm.mutex.unlock()

	// Calculate delay with exponential backoff
	delay_ms := lkm.reconnect_delay_ms * (1 << current_attempts) // 5s, 10s, 20s, 40s, 80s
	utils.info(cid, 'Attempting reconnection ${current_attempts + 1}/${lkm.max_reconnect_attempts} for ${stream_identifier} in ${delay_ms}ms',
		@FILE, @LINE.int())

	// Wait before reconnecting
	time.sleep(delay_ms * time.millisecond)

	// Attempt to reconnect
	if !lkm.is_running {
		utils.info(cid, 'Kline manager stopped, cancelling reconnection for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}

	// Create new client with conservative options to improve stability
	client_opt := websocket.ClientOpt{
		read_timeout:  30 * time.second  // Longer timeout for stability
		write_timeout: 15 * time.second  // Conservative write timeout
		logger:        &log.Log{}
	}

	mut new_client := websocket.new_client(original_url, client_opt) or {
		utils.error(cid, 'Failed to create new client for reconnection: ${err}', @FILE,
			@LINE.int())
		return
	}

	// Set up event handlers
	unsafe {
		new_client.on_open(lkm.on_kline_open)
		new_client.on_message(lkm.on_kline_message)
		new_client.on_error(lkm.on_kline_error)
		new_client.on_close(lkm.on_kline_close)
	}
	// Attempt connection
	new_client.connect() or {
		utils.error(cid, 'Failed to reconnect to ${original_url}: ${err}', @FILE, @LINE.int())
		// Schedule another reconnection attempt
		if current_attempts + 1 < lkm.max_reconnect_attempts {
			spawn lkm.attempt_reconnection_safe(original_url)
		}
		return
	}

	// Add a small delay to ensure connection is fully established before replacement
	time.sleep(200 * time.millisecond)

	// Success - replace the old client instead of appending
	lkm.mutex.@lock()
	defer { lkm.mutex.unlock() }

	// Find and replace the old client for this URL
	mut client_replaced := false
	for i, url in lkm.client_urls {
		if url == original_url && i < lkm.clients.len {
			// Store reference to old client before replacement
			mut old_client := lkm.clients[i]

			// Validate that we're not replacing the same client (avoid race conditions)
			if old_client.uri.str() == new_client.uri.str() {
				// Check if this is actually a different client instance
				if &old_client == &new_client {
					utils.debug(cid, 'Attempted to replace client with itself, skipping', @FILE, @LINE.int())
					client_replaced = true
					break
				}
			}

			// Replace with new client first
			lkm.clients[i] = new_client
			client_replaced = true

			// Now safely close the old client (in a separate goroutine to avoid blocking)
			spawn fn [mut old_client, cid] () {
				// Add delay to ensure new client is fully established
				time.sleep(300 * time.millisecond)

				// Attempt to close old client gracefully
				old_client.close(1000, 'Reconnection') or {
					utils.debug(cid, 'Old client was already closed or invalid: ${err}', @FILE, @LINE.int())
				}
				utils.debug(cid, 'Attempted to close old client during reconnection', @FILE, @LINE.int())
			}()
			break
		}
	}

	// If we couldn't find the old client, add the new one (shouldn't happen normally)
	if !client_replaced {
		lkm.clients << new_client
		lkm.client_urls << original_url
		utils.warn(cid, 'Could not find old client to replace, added new client for ${stream_identifier}',
			@FILE, @LINE.int())
	}

	// Reset attempt counter on success (with mutex protection)
	lkm.mutex.@lock()
	lkm.reconnect_attempts[stream_identifier] = 0
	lkm.mutex.unlock()
	utils.info(cid, 'Successfully reconnected to ${stream_identifier}', @FILE, @LINE.int())
}

// stop_kline_streams stops all kline stream connections (deprecated - use disconnect_all)
pub fn (mut lkm LiveKlineManager) stop_kline_streams() {
	lkm.disconnect_all()
}
