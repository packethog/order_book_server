-- DoubleZero Top-of-Book Feed dissector (v0.1.0)
--
-- Based on the Top-of-Book Feed + Reference Data Distribution v0.1.0 specs:
--   https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/v0.1.0.md
--   https://github.com/malbeclabs/edge-feed-spec/blob/main/reference-data/v0.1.0.md
--
-- Install:
--   Linux/Mac: copy to ~/.local/lib/wireshark/plugins/ (Wireshark 4.x)
--              or ~/.wireshark/plugins/ (older)
--   Or load ad-hoc: tshark -X lua_script:dz_topofbook.lua -f "udp port 31001" -i doublezero1
--
-- Configure UDP ports in Wireshark via:
--   Edit -> Preferences -> Protocols -> DZ-TOB
-- or pass on command line:
--   tshark -o dz_tob.marketdata_port:31001 -o dz_tob.refdata_port:31002 ...

local dz = Proto("dz_tob", "DoubleZero Top-of-Book")

-- ============================================================================
-- Field declarations
-- ============================================================================

local f = dz.fields

-- Frame header (24 bytes)
f.magic           = ProtoField.uint16("dz_tob.magic",        "Magic",           base.HEX)
f.schema_version  = ProtoField.uint8 ("dz_tob.schema_ver",   "Schema Version",  base.DEC)
f.channel_id      = ProtoField.uint8 ("dz_tob.channel_id",   "Channel ID",      base.DEC)
f.sequence        = ProtoField.uint64("dz_tob.seq",          "Sequence Number", base.DEC)
f.send_timestamp  = ProtoField.uint64("dz_tob.send_ts",      "Send Timestamp",  base.DEC)
f.msg_count       = ProtoField.uint8 ("dz_tob.msg_count",    "Message Count",   base.DEC)
f.frame_length    = ProtoField.uint16("dz_tob.frame_length", "Frame Length",    base.DEC)

-- App message header (4 bytes)
f.msg_type        = ProtoField.uint8 ("dz_tob.msg_type",   "Message Type",   base.HEX)
f.msg_length      = ProtoField.uint8 ("dz_tob.msg_length", "Message Length", base.DEC)
f.msg_flags       = ProtoField.uint16("dz_tob.msg_flags",  "Flags",          base.HEX)
f.msg_flag_snap   = ProtoField.bool  ("dz_tob.flag_snapshot", "Snapshot", 16, nil, 0x0001)

-- Quote (0x03)
f.q_inst_id       = ProtoField.uint32("dz_tob.quote.instrument_id", "Instrument ID", base.DEC)
f.q_source_id     = ProtoField.uint16("dz_tob.quote.source_id",     "Source ID",     base.DEC)
f.q_update_flags  = ProtoField.uint8 ("dz_tob.quote.update_flags",  "Update Flags",  base.HEX)
f.q_bid_updated   = ProtoField.bool  ("dz_tob.quote.bid_updated",   "Bid Updated", 8, nil, 0x01)
f.q_ask_updated   = ProtoField.bool  ("dz_tob.quote.ask_updated",   "Ask Updated", 8, nil, 0x02)
f.q_bid_gone      = ProtoField.bool  ("dz_tob.quote.bid_gone",      "Bid Gone",    8, nil, 0x04)
f.q_ask_gone      = ProtoField.bool  ("dz_tob.quote.ask_gone",      "Ask Gone",    8, nil, 0x08)
f.q_src_ts        = ProtoField.uint64("dz_tob.quote.source_ts",     "Source Timestamp", base.DEC)
f.q_bid_price     = ProtoField.int64 ("dz_tob.quote.bid_price",     "Bid Price")
f.q_bid_qty       = ProtoField.uint64("dz_tob.quote.bid_qty",       "Bid Quantity")
f.q_ask_price     = ProtoField.int64 ("dz_tob.quote.ask_price",     "Ask Price")
f.q_ask_qty       = ProtoField.uint64("dz_tob.quote.ask_qty",       "Ask Quantity")
f.q_bid_cnt       = ProtoField.uint16("dz_tob.quote.bid_source_count", "Bid Source Count", base.DEC)
f.q_ask_cnt       = ProtoField.uint16("dz_tob.quote.ask_source_count", "Ask Source Count", base.DEC)

-- Trade (0x04)
f.t_inst_id       = ProtoField.uint32("dz_tob.trade.instrument_id",   "Instrument ID", base.DEC)
f.t_source_id     = ProtoField.uint16("dz_tob.trade.source_id",       "Source ID",     base.DEC)
f.t_aggressor     = ProtoField.uint8 ("dz_tob.trade.aggressor_side",  "Aggressor Side", base.DEC,
                                      { [0]="Unknown", [1]="Buy", [2]="Sell" })
f.t_flags         = ProtoField.uint8 ("dz_tob.trade.trade_flags",     "Trade Flags", base.HEX)
f.t_src_ts        = ProtoField.uint64("dz_tob.trade.source_ts",       "Source Timestamp", base.DEC)
f.t_price         = ProtoField.int64 ("dz_tob.trade.price",           "Trade Price")
f.t_qty           = ProtoField.uint64("dz_tob.trade.qty",             "Trade Quantity")
f.t_trade_id      = ProtoField.uint64("dz_tob.trade.trade_id",        "Trade ID")
f.t_cum_vol       = ProtoField.uint64("dz_tob.trade.cumulative_volume", "Cumulative Volume")

-- Heartbeat (0x01)
f.hb_channel_id   = ProtoField.uint8 ("dz_tob.heartbeat.channel_id", "Channel ID", base.DEC)
f.hb_timestamp    = ProtoField.uint64("dz_tob.heartbeat.timestamp",  "Timestamp",  base.DEC)

-- ChannelReset (0x05)
f.cr_timestamp    = ProtoField.uint64("dz_tob.channel_reset.timestamp", "Timestamp", base.DEC)

-- EndOfSession (0x06)
f.eos_timestamp   = ProtoField.uint64("dz_tob.end_of_session.timestamp", "Timestamp", base.DEC)

-- InstrumentDefinition (0x02)
f.id_inst_id        = ProtoField.uint32("dz_tob.idef.instrument_id",  "Instrument ID", base.DEC)
f.id_symbol         = ProtoField.string("dz_tob.idef.symbol",         "Symbol")
f.id_leg1           = ProtoField.string("dz_tob.idef.leg1",           "Leg 1")
f.id_leg2           = ProtoField.string("dz_tob.idef.leg2",           "Leg 2")
f.id_asset_class    = ProtoField.uint8 ("dz_tob.idef.asset_class",    "Asset Class", base.DEC,
                                        { [0]="Unknown", [1]="Crypto Spot", [2]="Prediction Binary",
                                          [3]="Prediction Scalar", [4]="Prediction Categorical" })
f.id_price_exp      = ProtoField.int8  ("dz_tob.idef.price_exponent", "Price Exponent")
f.id_qty_exp        = ProtoField.int8  ("dz_tob.idef.qty_exponent",   "Qty Exponent")
f.id_market_model   = ProtoField.uint8 ("dz_tob.idef.market_model",   "Market Model", base.DEC,
                                        { [0]="Unknown", [1]="CLOB", [2]="AMM" })
f.id_tick_size      = ProtoField.int64 ("dz_tob.idef.tick_size",      "Tick Size")
f.id_lot_size       = ProtoField.uint64("dz_tob.idef.lot_size",       "Lot Size")
f.id_contract_value = ProtoField.uint64("dz_tob.idef.contract_value", "Contract Value")
f.id_expiry         = ProtoField.uint64("dz_tob.idef.expiry",         "Expiry")
f.id_settle_type    = ProtoField.uint8 ("dz_tob.idef.settle_type",    "Settle Type", base.DEC,
                                        { [0]="N/A", [1]="Cash", [2]="Physical" })
f.id_price_bound    = ProtoField.uint8 ("dz_tob.idef.price_bound",    "Price Bound", base.DEC,
                                        { [0]="Unbounded", [1]="Bounded [0,1]", [2]="Non-negative" })
f.id_manifest_seq   = ProtoField.uint16("dz_tob.idef.manifest_seq",   "Manifest Seq", base.DEC)

-- ManifestSummary (0x07)
f.ms_channel_id     = ProtoField.uint8 ("dz_tob.manifest.channel_id",       "Channel ID", base.DEC)
f.ms_manifest_seq   = ProtoField.uint16("dz_tob.manifest.seq",              "Manifest Seq", base.DEC)
f.ms_inst_count     = ProtoField.uint32("dz_tob.manifest.instrument_count", "Instrument Count", base.DEC)
f.ms_timestamp      = ProtoField.uint64("dz_tob.manifest.timestamp",        "Timestamp", base.DEC)

-- ============================================================================
-- Constants
-- ============================================================================

local MAGIC = 0x445A
local FRAME_HEADER_SIZE = 24
local APP_MSG_HEADER_SIZE = 4

local MSG_TYPE_HEARTBEAT        = 0x01
local MSG_TYPE_INSTRUMENT_DEF   = 0x02
local MSG_TYPE_QUOTE            = 0x03
local MSG_TYPE_TRADE            = 0x04
local MSG_TYPE_CHANNEL_RESET    = 0x05
local MSG_TYPE_END_OF_SESSION   = 0x06
local MSG_TYPE_MANIFEST_SUMMARY = 0x07

local MSG_TYPE_NAMES = {
    [0x01] = "Heartbeat",
    [0x02] = "InstrumentDefinition",
    [0x03] = "Quote",
    [0x04] = "Trade",
    [0x05] = "ChannelReset",
    [0x06] = "EndOfSession",
    [0x07] = "ManifestSummary",
}

-- ============================================================================
-- Preferences
-- ============================================================================

dz.prefs.marketdata_port = Pref.uint("Marketdata UDP port", 31001, "UDP port for Quote/Trade/Heartbeat/EndOfSession")
dz.prefs.refdata_port = Pref.uint("Refdata UDP port", 31002, "UDP port for InstrumentDefinition/ManifestSummary/ChannelReset")

-- ============================================================================
-- Per-message dissectors
-- ============================================================================

local function dissect_quote(body, subtree)
    subtree:add_le(f.q_inst_id,       body(4, 4))
    subtree:add_le(f.q_source_id,     body(8, 2))
    local uf = subtree:add_le(f.q_update_flags, body(10, 1))
    uf:add_le(f.q_bid_updated, body(10, 1))
    uf:add_le(f.q_ask_updated, body(10, 1))
    uf:add_le(f.q_bid_gone,    body(10, 1))
    uf:add_le(f.q_ask_gone,    body(10, 1))
    subtree:add_le(f.q_src_ts,    body(12, 8))
    subtree:add_le(f.q_bid_price, body(20, 8))
    subtree:add_le(f.q_bid_qty,   body(28, 8))
    subtree:add_le(f.q_ask_price, body(36, 8))
    subtree:add_le(f.q_ask_qty,   body(44, 8))
    subtree:add_le(f.q_bid_cnt,   body(52, 2))
    subtree:add_le(f.q_ask_cnt,   body(54, 2))

    local inst_id = body(4, 4):le_uint()
    local bid_px  = body(20, 8):le_int64()
    local ask_px  = body(36, 8):le_int64()
    return string.format("Quote inst=%d bid=%s ask=%s", inst_id, tostring(bid_px), tostring(ask_px))
end

local function dissect_trade(body, subtree)
    subtree:add_le(f.t_inst_id,   body(4, 4))
    subtree:add_le(f.t_source_id, body(8, 2))
    subtree:add_le(f.t_aggressor, body(10, 1))
    subtree:add_le(f.t_flags,     body(11, 1))
    subtree:add_le(f.t_src_ts,    body(12, 8))
    subtree:add_le(f.t_price,     body(20, 8))
    subtree:add_le(f.t_qty,       body(28, 8))
    subtree:add_le(f.t_trade_id,  body(36, 8))
    subtree:add_le(f.t_cum_vol,   body(44, 8))

    local inst_id = body(4, 4):le_uint()
    local px      = body(20, 8):le_int64()
    local qty     = body(28, 8):le_uint64()
    return string.format("Trade inst=%d px=%s qty=%s", inst_id, tostring(px), tostring(qty))
end

local function dissect_heartbeat(body, subtree)
    subtree:add_le(f.hb_channel_id, body(4, 1))
    subtree:add_le(f.hb_timestamp,  body(8, 8))
    return "Heartbeat"
end

local function dissect_channel_reset(body, subtree)
    subtree:add_le(f.cr_timestamp, body(4, 8))
    return "ChannelReset"
end

local function dissect_end_of_session(body, subtree)
    subtree:add_le(f.eos_timestamp, body(4, 8))
    return "EndOfSession"
end

local function dissect_instrument_definition(body, subtree)
    subtree:add_le(f.id_inst_id,        body(4, 4))
    subtree:add   (f.id_symbol,         body(8, 16))
    subtree:add   (f.id_leg1,           body(24, 8))
    subtree:add   (f.id_leg2,           body(32, 8))
    subtree:add_le(f.id_asset_class,    body(40, 1))
    subtree:add_le(f.id_price_exp,      body(41, 1))
    subtree:add_le(f.id_qty_exp,        body(42, 1))
    subtree:add_le(f.id_market_model,   body(43, 1))
    subtree:add_le(f.id_tick_size,      body(44, 8))
    subtree:add_le(f.id_lot_size,       body(52, 8))
    subtree:add_le(f.id_contract_value, body(60, 8))
    subtree:add_le(f.id_expiry,         body(68, 8))
    subtree:add_le(f.id_settle_type,    body(76, 1))
    subtree:add_le(f.id_price_bound,    body(77, 1))
    subtree:add_le(f.id_manifest_seq,   body(78, 2))

    local inst_id = body(4, 4):le_uint()
    local symbol  = body(8, 16):stringz()
    return string.format("InstrumentDefinition inst=%d sym=%s", inst_id, symbol)
end

local function dissect_manifest_summary(body, subtree)
    subtree:add_le(f.ms_channel_id,   body(4, 1))
    subtree:add_le(f.ms_manifest_seq, body(8, 2))
    subtree:add_le(f.ms_inst_count,   body(12, 4))
    subtree:add_le(f.ms_timestamp,    body(16, 8))

    local seq   = body(8, 2):le_uint()
    local count = body(12, 4):le_uint()
    return string.format("ManifestSummary seq=%d count=%d", seq, count)
end

local DISSECTORS = {
    [MSG_TYPE_HEARTBEAT]        = dissect_heartbeat,
    [MSG_TYPE_INSTRUMENT_DEF]   = dissect_instrument_definition,
    [MSG_TYPE_QUOTE]            = dissect_quote,
    [MSG_TYPE_TRADE]            = dissect_trade,
    [MSG_TYPE_CHANNEL_RESET]    = dissect_channel_reset,
    [MSG_TYPE_END_OF_SESSION]   = dissect_end_of_session,
    [MSG_TYPE_MANIFEST_SUMMARY] = dissect_manifest_summary,
}

-- ============================================================================
-- Main dissector
-- ============================================================================

function dz.dissector(tvb, pinfo, tree)
    local len = tvb:len()
    if len < FRAME_HEADER_SIZE then
        return 0
    end

    local magic = tvb(0, 2):le_uint()
    if magic ~= MAGIC then
        return 0
    end

    pinfo.cols.protocol = "DZ-TOB"

    local root = tree:add(dz, tvb(), "DoubleZero Top-of-Book Frame")

    -- Frame header
    local hdr = root:add(dz, tvb(0, FRAME_HEADER_SIZE), "Frame Header")
    hdr:add_le(f.magic,          tvb(0, 2))
    hdr:add_le(f.schema_version, tvb(2, 1))
    hdr:add_le(f.channel_id,     tvb(3, 1))
    hdr:add_le(f.sequence,       tvb(4, 8))
    hdr:add_le(f.send_timestamp, tvb(12, 8))
    hdr:add_le(f.msg_count,      tvb(20, 1))
    hdr:add_le(f.frame_length,   tvb(22, 2))

    local schema_ver = tvb(2, 1):uint()
    local channel_id = tvb(3, 1):uint()
    local seq        = tvb(4, 8):le_uint64()
    local msg_count  = tvb(20, 1):uint()

    local info_parts = {}
    table.insert(info_parts, string.format("ch=%d seq=%s msgs=%d", channel_id, tostring(seq), msg_count))

    -- Iterate messages
    local offset = FRAME_HEADER_SIZE
    for i = 0, msg_count - 1 do
        if offset + APP_MSG_HEADER_SIZE > len then
            root:add_expert_info(PI_MALFORMED, PI_ERROR, "Truncated app message header")
            break
        end

        local msg_type = tvb(offset, 1):uint()
        local msg_len  = tvb(offset + 1, 1):uint()

        if offset + msg_len > len then
            root:add_expert_info(PI_MALFORMED, PI_ERROR,
                string.format("Truncated message body (need %d, have %d)", msg_len, len - offset))
            break
        end

        local msg_name = MSG_TYPE_NAMES[msg_type] or string.format("Unknown(0x%02X)", msg_type)
        local msg_tree = root:add(dz, tvb(offset, msg_len),
            string.format("Message %d: %s", i, msg_name))

        -- App header fields
        msg_tree:add_le(f.msg_type,   tvb(offset,     1))
        msg_tree:add_le(f.msg_length, tvb(offset + 1, 1))
        local flags_item = msg_tree:add_le(f.msg_flags, tvb(offset + 2, 2))
        flags_item:add_le(f.msg_flag_snap, tvb(offset + 2, 2))

        -- Per-message body
        local dissector = DISSECTORS[msg_type]
        if dissector then
            local body_tvb = tvb(offset, msg_len)
            local summary = dissector(body_tvb, msg_tree)
            if summary then
                table.insert(info_parts, summary)
            end
        else
            msg_tree:add_expert_info(PI_UNDECODED, PI_NOTE,
                string.format("Unknown message type 0x%02X", msg_type))
        end

        offset = offset + msg_len
    end

    pinfo.cols.info = table.concat(info_parts, " | ")
    return len
end

-- ============================================================================
-- Register
-- ============================================================================

local udp_table = DissectorTable.get("udp.port")
local registered_ports = {}

local function register_ports()
    -- Unregister any previously registered ports
    for port, _ in pairs(registered_ports) do
        udp_table:remove(port, dz)
    end
    registered_ports = {}

    -- Register current preference values
    if dz.prefs.marketdata_port > 0 then
        udp_table:add(dz.prefs.marketdata_port, dz)
        registered_ports[dz.prefs.marketdata_port] = true
    end
    if dz.prefs.refdata_port > 0 then
        udp_table:add(dz.prefs.refdata_port, dz)
        registered_ports[dz.prefs.refdata_port] = true
    end
end

function dz.init()
    register_ports()
end

register_ports()
