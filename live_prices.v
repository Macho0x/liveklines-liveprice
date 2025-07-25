module connections

import net.websocket
import time
import json
import sync
import configs
import utils
import database
import log

// BinancePriceTickerMessage represents Binance 24hr ticker price change statistics
pub struct BinancePriceTickerMessage {
pub:
	stream string             @[json: 'stream']
	data   BinancePriceTicker @[json: 'data']
}

pub struct BinancePriceTicker {
pub:
	event_type                string @[json: 'e'] // Event type (24hrTicker)
	event_time                i64    @[json: 'E'] // Event time
	symbol                    string @[json: 's'] // Symbol
	price_change              string @[json: 'p'] // Price change
	price_change_percent      string @[json: 'P'] // Price change percent
	weighted_avg_price        string @[json: 'w'] // Weighted average price
	first_trade_price         string @[json: 'x'] // First trade price
	last_price                string @[json: 'c'] // Last price
	last_qty                  string @[json: 'Q'] // Last quantity
	best_bid_price            string @[json: 'b'] // Best bid price
	best_bid_qty              string @[json: 'B'] // Best bid quantity
	best_ask_price            string @[json: 'a'] // Best ask price
	best_ask_qty              string @[json: 'A'] // Best ask quantity
	open_price                string @[json: 'o'] // Open price
	high_price                string @[json: 'h'] // High price
	low_price                 string @[json: 'l'] // Low price
	total_traded_base_volume  string @[json: 'v'] // Total traded base asset volume
	total_traded_quote_volume string @[json: 'q'] // Total traded quote asset volume
	statistics_open_time      i64    @[json: 'O'] // Statistics open time
	statistics_close_time     i64    @[json: 'C'] // Statistics close time
	first_trade_id            i64    @[json: 'F'] // First trade ID
	last_trade_id             i64    @[json: 'L'] // Last trade ID
	total_trades_count        i64    @[json: 'n'] // Total number of trades
}

// LivePriceManager manages real-time price WebSocket connections (UUID system)
@[heap]
pub struct LivePriceManager {
pub mut:
	clients                []websocket.Client
	client_urls            []string // track URLs for each client index
	uuid_trading_manager   &database.UUIDTradingManager
	is_running             bool
	is_testnet_mode        bool
	reconnect_attempts     map[string]int  // track reconnection attempts per client
	reconnecting_streams   map[string]bool // prevent duplicate reconnections
	max_reconnect_attempts int = 5
	reconnect_delay_ms     int = 5000 // 5 seconds initial delay
	mutex                  sync.Mutex                 // protect client operations
	reconnection_manager   &utils.ReconnectionManager // manage reconnection goroutines
}

// new_live_price_manager creates a new live price manager with UUID support
pub fn new_live_price_manager(mut uuid_trading_manager database.UUIDTradingManager) LivePriceManager {
	return LivePriceManager{
		clients:                []websocket.Client{}
		client_urls:            []string{}
		uuid_trading_manager:   &uuid_trading_manager
		is_running:             false
		is_testnet_mode:        false
		reconnect_attempts:     map[string]int{}
		reconnecting_streams:   map[string]bool{}
		max_reconnect_attempts: 5
		reconnect_delay_ms:     5000
		mutex:                  sync.new_mutex()
		reconnection_manager:   utils.new_reconnection_manager(10) // Max 10 concurrent reconnections
	}
}

// connect_price_streams establishes WebSocket connections for price ticker streams
pub fn (mut lpm LivePriceManager) connect_price_streams(is_testnet_mode bool) {
	lpm.is_testnet_mode = is_testnet_mode
	connection_conf := configs.load_connection_config(is_testnet_mode)
	base_ws_url := connection_conf.urls.websocket
	connect_cid := 'live_prices_connect'

	// Conservative client options for better stability
	client_opt := websocket.ClientOpt{
		read_timeout:  30 * time.second  // Longer timeout for stability
		write_timeout: 15 * time.second  // Conservative write timeout
		logger:        &log.Log{}
	}

	// Get all symbols that need price monitoring
	trading_symbols := configs.get_enabled_trading_symbols()
	data_symbols := configs.get_enabled_data_symbols()

	// Combine and deduplicate symbols
	mut all_symbols := map[string]bool{}
	for symbol_config in trading_symbols {
		all_symbols[symbol_config.symbol] = true
	}
	for symbol_config in data_symbols {
		all_symbols[symbol_config.symbol] = true
	}

	utils.info(connect_cid, 'Connecting to price streams for ${all_symbols.len} symbols (UUID system)',
		@FILE, @LINE.int())

	for symbol, _ in all_symbols {
		// Binance individual symbol ticker stream: <symbol>@ticker
		stream_path := '${symbol.to_lower()}@ticker'
		full_ws_url := '${base_ws_url}/${stream_path}'
		utils.info(connect_cid, 'Attempting to connect to price stream: ${full_ws_url}',
			@FILE, @LINE.int())

		mut client := websocket.new_client(full_ws_url, client_opt) or {
			utils.error(connect_cid, 'Failed to create price client for ${full_ws_url}: ${err}',
				@FILE, @LINE.int())
			continue
		}

		unsafe {
			client.on_open(lpm.on_price_open)
			client.on_message(lpm.on_price_message)
			client.on_error(lpm.on_price_error)
			client.on_close(lpm.on_price_close)
		}
		client.connect() or {
			utils.error(connect_cid, 'Failed to connect to ${full_ws_url}: ${err}', @FILE,
				@LINE.int())
			continue
		}

		lpm.mutex.@lock()
		lpm.clients << client
		lpm.client_urls << full_ws_url
		lpm.mutex.unlock()
		utils.info(connect_cid, 'Successfully initiated price connection to ${full_ws_url}',
			@FILE, @LINE.int())
	}

	lpm.is_running = true
	utils.info(connect_cid, 'All ${lpm.clients.len} price stream connections initiated with UUID system',
		@FILE, @LINE.int())
}

// disconnect_all gracefully disconnects all WebSocket connections and cleans up resources
pub fn (mut lpm LivePriceManager) disconnect_all() {
	cid := 'live_prices_disconnect_all'
	utils.info(cid, 'Disconnecting all price stream connections...', @FILE, @LINE.int())

	lpm.mutex.@lock()
	defer { lpm.mutex.unlock() }

	lpm.is_running = false

	// Close all WebSocket connections
	for mut client in lpm.clients {
		client.close(1000, 'Normal closure') or {
			utils.warn(cid, 'Failed to close WebSocket connection: ${err}', @FILE, @LINE.int())
		}
	}

	// Clear all tracking data
	lpm.clients.clear()
	lpm.client_urls.clear()
	lpm.reconnect_attempts.clear()
	lpm.reconnecting_streams.clear()

	// Shutdown reconnection manager
	lpm.reconnection_manager.shutdown(5)

	utils.info(cid, 'All price stream connections disconnected and resources cleaned up',
		@FILE, @LINE.int())
}

// WebSocket event handlers for price streams
fn (mut lpm LivePriceManager) on_price_open(mut client websocket.Client) ! {
	cid := 'price_ws_open:${client.uri.str()}'
	utils.info(cid, 'Price stream connection opened', @FILE, @LINE.int())

	// Send initial ping to confirm connection
	client.ping() or {
		utils.error(cid, 'Failed to send initial ping: ${err}', @FILE, @LINE.int())
		return err
	}

	// Add circuit breaker heartbeat with exponential backoff
	spawn fn [mut client, cid, mut lpm] () {
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
			if !lpm.is_running {
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
				if lpm.is_running {
					url := client.uri.str()
					spawn fn [mut lpm, url] () {
						time.sleep(2 * time.second) // Small delay to avoid race conditions
						lpm.attempt_reconnection_safe(url)
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
				if lpm.is_running {
					url := client.uri.str()

					// Add exponential backoff for reconnection attempts
					backoff_delay := if consecutive_failures == 1 {
						2 * time.second
					} else {
						5 * time.second
					}

					utils.debug(cid, 'Waiting ${backoff_delay / time.second}s before reconnection attempt', @FILE, @LINE.int())
					time.sleep(backoff_delay)

					if !lpm.reconnection_manager.attempt_reconnection(url, fn [mut lpm] (url string) {
						lpm.attempt_reconnection_safe(url)
					}) {
						utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
							@FILE, @LINE.int())
						// Get reconnection manager stats for debugging
						active_reconnections, active_goroutines, available_slots := lpm.reconnection_manager.get_stats()
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

fn (mut lpm LivePriceManager) on_price_message(mut client websocket.Client, msg &websocket.Message) ! {
	cid := 'price_ws_message:${client.uri.str()}'

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

	// Parse Binance price ticker message
	ticker_msg := json.decode(BinancePriceTickerMessage, msg.payload.bytestr()) or {
		utils.error(cid, 'Failed to parse price ticker message: ${err}', @FILE, @LINE.int())
		return
	}

	// Convert to database PriceUpdate format
	price_update := database.PriceUpdate{
		symbol:               ticker_msg.data.symbol
		price:                ticker_msg.data.last_price
		bid_price:            ticker_msg.data.best_bid_price
		ask_price:            ticker_msg.data.best_ask_price
		volume:               ticker_msg.data.total_traded_base_volume
		price_change:         ticker_msg.data.price_change
		price_change_percent: ticker_msg.data.price_change_percent
		high_price:           ticker_msg.data.high_price
		low_price:            ticker_msg.data.low_price
		open_price:           ticker_msg.data.open_price
		timestamp:            ticker_msg.data.event_time / 1000 // Convert to seconds
	}

	// Process through UUID trading manager (this will handle triggers, P&L updates, etc.)
	lpm.uuid_trading_manager.process_price_update(price_update.symbol, price_update.price,
		price_update.timestamp) or {
		utils.error(cid, 'Failed to process price update via UUID system for ${price_update.symbol}: ${err}',
			@FILE, @LINE.int())
	}

	utils.debug(cid, 'Processed price update via UUID system: ${price_update.symbol} = ${price_update.price}',
		@FILE, @LINE.int())
}

fn (mut lpm LivePriceManager) on_price_error(mut client websocket.Client, err string) ! {
	cid := 'price_ws_error:${client.uri.str()}'
	utils.error(cid, 'Price stream error: ${err}', @FILE, @LINE.int())

	// Attempt reconnection on error using managed reconnection
	if lpm.is_running {
		url := client.uri.str()
		if !lpm.reconnection_manager.attempt_reconnection(url, fn [mut lpm] (url string) {
			lpm.attempt_reconnection_safe(url)
		}) {
			utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
				@FILE, @LINE.int())
		}
	}
}

fn (mut lpm LivePriceManager) on_price_close(mut client websocket.Client, code int, reason string) ! {
	cid := 'price_ws_close:${client.uri.str()}'
	utils.info(cid, 'Price stream connection closed. Code: ${code}, Reason: ${reason}',
		@FILE, @LINE.int())

	// Only attempt reconnection for unexpected closures (not normal closure or reconnection)
	if lpm.is_running && code != 1000 && reason != 'Reconnection' {
		url := client.uri.str()
		utils.debug(cid, 'Unexpected closure, attempting reconnection for ${url}', @FILE, @LINE.int())
		if !lpm.reconnection_manager.attempt_reconnection(url, fn [mut lpm] (url string) {
			lpm.attempt_reconnection_safe(url)
		}) {
			utils.warn(cid, 'Failed to start reconnection for ${url} - at limit or already in progress',
				@FILE, @LINE.int())
		}
	} else {
		utils.debug(cid, 'Normal or intentional closure, no reconnection needed', @FILE, @LINE.int())
	}
}

// attempt_reconnection tries to reconnect a failed WebSocket connection
fn (mut lpm LivePriceManager) attempt_reconnection(original_url string) {
	cid := 'price_ws_reconnect'

	// Extract symbol from URL for tracking attempts
	url_parts := original_url.split('/')
	if url_parts.len == 0 {
		utils.error(cid, 'Cannot parse URL for reconnection: ${original_url}', @FILE,
			@LINE.int())
		return
	}

	stream_identifier := url_parts[url_parts.len - 1] // e.g., "btcusdt@ticker"

	// Prevent duplicate reconnection attempts
	lpm.mutex.@lock()
	if stream_identifier in lpm.reconnecting_streams && lpm.reconnecting_streams[stream_identifier] {
		lpm.mutex.unlock()
		utils.debug(cid, 'Reconnection already in progress for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}
	lpm.reconnecting_streams[stream_identifier] = true
	lpm.mutex.unlock()

	defer {
		lpm.mutex.@lock()
		lpm.reconnecting_streams[stream_identifier] = false
		lpm.mutex.unlock()
	}

	// Check reconnection attempts (with mutex protection)
	lpm.mutex.@lock()
	current_attempts := lpm.reconnect_attempts[stream_identifier] or { 0 }
	if current_attempts >= lpm.max_reconnect_attempts {
		lpm.mutex.unlock()
		utils.error(cid, 'Max reconnection attempts (${lpm.max_reconnect_attempts}) reached for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}

	// Increment attempt counter
	lpm.reconnect_attempts[stream_identifier] = current_attempts + 1
	lpm.mutex.unlock()

	// Calculate delay with exponential backoff
	delay_ms := lpm.reconnect_delay_ms * (1 << current_attempts) // 5s, 10s, 20s, 40s, 80s
	utils.info(cid, 'Attempting reconnection ${current_attempts + 1}/${lpm.max_reconnect_attempts} for ${stream_identifier} in ${delay_ms}ms',
		@FILE, @LINE.int())

	// Wait before reconnecting
	time.sleep(delay_ms * time.millisecond)

	// Attempt to reconnect
	if !lpm.is_running {
		utils.info(cid, 'Price manager stopped, cancelling reconnection for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}

	// Create new client with optimized options for Binance's new timeout rules
	client_opt := websocket.ClientOpt{
		read_timeout:  20 * time.second  // Shorter than Binance's 20s ping interval
		write_timeout: 10 * time.second  // Quick write timeout for responsiveness
		logger:        &log.Log{}
	}

	mut new_client := websocket.new_client(original_url, client_opt) or {
		utils.error(cid, 'Failed to create new client for reconnection: ${err}', @FILE,
			@LINE.int())
		return
	}

	// Set up event handlers
	unsafe {
		new_client.on_open(lpm.on_price_open)
		new_client.on_message(lpm.on_price_message)
		new_client.on_error(lpm.on_price_error)
		new_client.on_close(lpm.on_price_close)
	}
	// Attempt connection
	new_client.connect() or {
		utils.error(cid, 'Failed to reconnect to ${original_url}: ${err}', @FILE, @LINE.int())
		// Schedule another reconnection attempt
		if current_attempts + 1 < lpm.max_reconnect_attempts {
			spawn lpm.attempt_reconnection(original_url)
		}
		return
	}

	// Success - replace the old client instead of appending
	lpm.mutex.@lock()
	defer { lpm.mutex.unlock() }

	// Find and replace the old client for this URL
	mut client_replaced := false
	for i, url in lpm.client_urls {
		if url == original_url {
			// Store reference to old client before replacement
			mut old_client := lpm.clients[i]

			// Replace with new client first
			lpm.clients[i] = new_client
			client_replaced = true

			// Now safely close the old client (in a separate goroutine to avoid blocking)
			spawn fn [mut old_client, cid] () {
				// Add small delay to ensure new client is fully established
				time.sleep(100 * time.millisecond)
				old_client.close(1000, 'Reconnection') or {
					utils.debug(cid, 'Old client was already closed or invalid: ${err}', @FILE, @LINE.int())
				}
			}()
			break
		}
	}

	// If we couldn't find the old client, add the new one (shouldn't happen normally)
	if !client_replaced {
		lpm.clients << new_client
		lpm.client_urls << original_url
		utils.warn(cid, 'Could not find old client to replace, added new client for ${stream_identifier}',
			@FILE, @LINE.int())
	}

	// Reset attempt counter on success (with mutex protection)
	lpm.mutex.@lock()
	lpm.reconnect_attempts[stream_identifier] = 0
	lpm.mutex.unlock()
	utils.info(cid, 'Successfully reconnected to ${stream_identifier}', @FILE, @LINE.int())
}

// attempt_reconnection_safe tries to reconnect a failed WebSocket connection with duplicate prevention
fn (mut lpm LivePriceManager) attempt_reconnection_safe(original_url string) {
	cid := 'price_ws_reconnect'

	// Extract stream identifier from URL for tracking attempts
	url_parts := original_url.split('/')
	if url_parts.len == 0 {
		utils.error(cid, 'Cannot parse URL for reconnection: ${original_url}', @FILE,
			@LINE.int())
		return
	}

	stream_identifier := url_parts[url_parts.len - 1] // e.g., "btcusdt@ticker"

	// Prevent duplicate reconnection attempts
	lpm.mutex.@lock()
	if stream_identifier in lpm.reconnecting_streams && lpm.reconnecting_streams[stream_identifier] {
		lpm.mutex.unlock()
		utils.debug(cid, 'Reconnection already in progress for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}
	lpm.reconnecting_streams[stream_identifier] = true
	lpm.mutex.unlock()

	defer {
		lpm.mutex.@lock()
		lpm.reconnecting_streams[stream_identifier] = false
		lpm.mutex.unlock()
	}

	// Check reconnection attempts (with mutex protection)
	lpm.mutex.@lock()
	current_attempts := lpm.reconnect_attempts[stream_identifier] or { 0 }
	if current_attempts >= lpm.max_reconnect_attempts {
		lpm.mutex.unlock()
		utils.error(cid, 'Max reconnection attempts (${lpm.max_reconnect_attempts}) reached for ${stream_identifier}',
			@FILE, @LINE.int())
		return
	}

	// Increment attempt counter
	lpm.reconnect_attempts[stream_identifier] = current_attempts + 1
	lpm.mutex.unlock()

	// Calculate delay with exponential backoff
	delay_ms := lpm.reconnect_delay_ms * (1 << current_attempts) // 5s, 10s, 20s, 40s, 80s
	utils.info(cid, 'Attempting reconnection ${current_attempts + 1}/${lpm.max_reconnect_attempts} for ${stream_identifier} in ${delay_ms}ms',
		@FILE, @LINE.int())

	// Wait before reconnecting
	time.sleep(delay_ms * time.millisecond)

	// Attempt to reconnect
	if !lpm.is_running {
		utils.info(cid, 'Price manager stopped, cancelling reconnection for ${stream_identifier}',
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
		new_client.on_open(lpm.on_price_open)
		new_client.on_message(lpm.on_price_message)
		new_client.on_error(lpm.on_price_error)
		new_client.on_close(lpm.on_price_close)
	}
	// Attempt connection
	new_client.connect() or {
		utils.error(cid, 'Failed to reconnect to ${original_url}: ${err}', @FILE, @LINE.int())
		// Schedule another reconnection attempt
		if current_attempts + 1 < lpm.max_reconnect_attempts {
			spawn lpm.attempt_reconnection_safe(original_url)
		}
		return
	}

	// Add a small delay to ensure connection is fully established before replacement
	time.sleep(200 * time.millisecond)

	// Success - replace the old client instead of appending
	lpm.mutex.@lock()
	defer { lpm.mutex.unlock() }

	// Find and replace the old client for this URL
	mut client_replaced := false
	for i, url in lpm.client_urls {
		if url == original_url && i < lpm.clients.len {
			// Store reference to old client before replacement
			mut old_client := lpm.clients[i]

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
			lpm.clients[i] = new_client
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
		lpm.clients << new_client
		lpm.client_urls << original_url
		utils.warn(cid, 'Could not find old client to replace, added new client for ${stream_identifier}',
			@FILE, @LINE.int())
	}

	// Reset attempt counter on success (with mutex protection)
	lpm.mutex.@lock()
	lpm.reconnect_attempts[stream_identifier] = 0
	lpm.mutex.unlock()
	utils.info(cid, 'Successfully reconnected to ${stream_identifier}', @FILE, @LINE.int())
}

// stop_price_streams stops all price stream connections (deprecated - use disconnect_all)
pub fn (mut lpm LivePriceManager) stop_price_streams() {
	lpm.disconnect_all()
}
