-- DoubleZero Depth-of-Book Feed dissector (v0.1.0)
--
-- Wire spec: https://github.com/malbeclabs/edge-feed-spec/blob/main/depth-of-book/spec.md
--
-- Install:
--   Linux/Mac: copy to ~/.local/lib/wireshark/plugins/ (Wireshark 4.x)
--              or ~/.wireshark/plugins/ (older)
--   Or load ad-hoc: tshark -X lua_script:dz_depthofbook.lua -f "udp port 6000" -i lo
--
-- Configure UDP ports in Wireshark via:
--   Edit -> Preferences -> Protocols -> DZ-DoB
-- or pass on command line:
--   tshark -o dz_dob.mktdata_port:6000 -o dz_dob.refdata_port:6001 ...

local dz = Proto("dz_dob", "DoubleZero Depth-of-Book")

-- ============================================================================
-- Field declarations
-- ============================================================================

local f = dz.fields

-- Frame header (24 bytes)
f.magic           = ProtoField.uint16("dz_dob.magic",        "Magic",           base.HEX)
f.schema_version  = ProtoField.uint8 ("dz_dob.schema_ver",   "Schema Version",  base.DEC)
f.channel_id      = ProtoField.uint8 ("dz_dob.channel_id",   "Channel ID",      base.DEC)
f.sequence        = ProtoField.uint64("dz_dob.seq",          "Sequence Number", base.DEC)
f.send_timestamp  = ProtoField.uint64("dz_dob.send_ts",      "Send Timestamp",  base.DEC)
f.msg_count       = ProtoField.uint8 ("dz_dob.msg_count",    "Message Count",   base.DEC)
f.reset_count     = ProtoField.uint8 ("dz_dob.reset_count",  "Reset Count",     base.DEC)
f.frame_length    = ProtoField.uint16("dz_dob.frame_length", "Frame Length",    base.DEC)

-- App message header (4 bytes)
f.msg_type        = ProtoField.uint8 ("dz_dob.msg_type",   "Message Type",   base.HEX)
f.msg_length      = ProtoField.uint8 ("dz_dob.msg_length", "Message Length", base.DEC)
f.msg_flags       = ProtoField.uint16("dz_dob.msg_flags",  "Flags",          base.HEX)
f.msg_flag_snap   = ProtoField.bool  ("dz_dob.flag_snapshot", "Snapshot", 16, nil, 0x0001)

-- Heartbeat (0x01) — shared with TOB
f.hb_channel_id   = ProtoField.uint8 ("dz_dob.heartbeat.channel_id", "Channel ID", base.DEC)
f.hb_timestamp    = ProtoField.uint64("dz_dob.heartbeat.timestamp",  "Timestamp",  base.DEC)

-- InstrumentDefinition (0x02) — shared with TOB
f.id_inst_id        = ProtoField.uint32("dz_dob.idef.instrument_id",  "Instrument ID", base.DEC)
f.id_symbol         = ProtoField.string("dz_dob.idef.symbol",         "Symbol")
f.id_leg1           = ProtoField.string("dz_dob.idef.leg1",           "Leg 1")
f.id_leg2           = ProtoField.string("dz_dob.idef.leg2",           "Leg 2")
f.id_asset_class    = ProtoField.uint8 ("dz_dob.idef.asset_class",    "Asset Class", base.DEC,
                                        { [0]="Unknown", [1]="Crypto Spot", [2]="Prediction Binary",
                                          [3]="Prediction Scalar", [4]="Prediction Categorical" })
f.id_price_exp      = ProtoField.int8  ("dz_dob.idef.price_exponent", "Price Exponent")
f.id_qty_exp        = ProtoField.int8  ("dz_dob.idef.qty_exponent",   "Qty Exponent")
f.id_market_model   = ProtoField.uint8 ("dz_dob.idef.market_model",   "Market Model", base.DEC,
                                        { [0]="Unknown", [1]="CLOB", [2]="AMM" })
f.id_tick_size      = ProtoField.int64 ("dz_dob.idef.tick_size",      "Tick Size")
f.id_lot_size       = ProtoField.uint64("dz_dob.idef.lot_size",       "Lot Size")
f.id_contract_value = ProtoField.uint64("dz_dob.idef.contract_value", "Contract Value")
f.id_expiry         = ProtoField.uint64("dz_dob.idef.expiry",         "Expiry")
f.id_settle_type    = ProtoField.uint8 ("dz_dob.idef.settle_type",    "Settle Type", base.DEC,
                                        { [0]="N/A", [1]="Cash", [2]="Physical" })
f.id_price_bound    = ProtoField.uint8 ("dz_dob.idef.price_bound",    "Price Bound", base.DEC,
                                        { [0]="Unbounded", [1]="Bounded [0,1]", [2]="Non-negative" })
f.id_manifest_seq   = ProtoField.uint16("dz_dob.idef.manifest_seq",   "Manifest Seq", base.DEC)

-- Trade (0x04) — shared with TOB
f.t_inst_id       = ProtoField.uint32("dz_dob.trade.instrument_id",   "Instrument ID", base.DEC)
f.t_source_id     = ProtoField.uint16("dz_dob.trade.source_id",       "Source ID",     base.DEC)
f.t_aggressor     = ProtoField.uint8 ("dz_dob.trade.aggressor_side",  "Aggressor Side", base.DEC,
                                      { [0]="Unknown", [1]="Buy", [2]="Sell" })
f.t_flags         = ProtoField.uint8 ("dz_dob.trade.trade_flags",     "Trade Flags",   base.HEX)
f.t_src_ts        = ProtoField.uint64("dz_dob.trade.source_ts",       "Source Timestamp", base.DEC)
f.t_price         = ProtoField.int64 ("dz_dob.trade.price",           "Trade Price")
f.t_qty           = ProtoField.uint64("dz_dob.trade.qty",             "Trade Quantity")
f.t_trade_id      = ProtoField.uint64("dz_dob.trade.trade_id",        "Trade ID")
f.t_cum_vol       = ProtoField.uint64("dz_dob.trade.cumulative_volume", "Cumulative Volume")

-- EndOfSession (0x06) — shared with TOB
f.eos_timestamp   = ProtoField.uint64("dz_dob.end_of_session.timestamp", "Timestamp", base.DEC)

-- ManifestSummary (0x07) — shared with TOB
f.ms_channel_id   = ProtoField.uint8 ("dz_dob.manifest.channel_id",       "Channel ID",       base.DEC)
f.ms_manifest_seq = ProtoField.uint16("dz_dob.manifest.seq",              "Manifest Seq",     base.DEC)
f.ms_inst_count   = ProtoField.uint32("dz_dob.manifest.instrument_count", "Instrument Count", base.DEC)
f.ms_timestamp    = ProtoField.uint64("dz_dob.manifest.timestamp",        "Timestamp",        base.DEC)

-- OrderAdd (0x10)
f.oa_inst_id      = ProtoField.uint32("dz_dob.oa.instrument_id",       "Instrument ID",      base.DEC)
f.oa_source_id    = ProtoField.uint16("dz_dob.oa.source_id",           "Source ID",          base.DEC)
f.oa_side         = ProtoField.uint8 ("dz_dob.oa.side",                "Side",               base.DEC,
                                      { [0]="Bid", [1]="Ask" })
f.oa_order_flags  = ProtoField.uint8 ("dz_dob.oa.order_flags",         "Order Flags",        base.HEX)
f.oa_flag_post    = ProtoField.bool  ("dz_dob.oa.flag_post_only",      "Post Only",      8, nil, 0x01)
f.oa_flag_reduce  = ProtoField.bool  ("dz_dob.oa.flag_reduce_only",    "Reduce Only",    8, nil, 0x02)
f.oa_flag_hidden  = ProtoField.bool  ("dz_dob.oa.flag_hidden",         "Hidden",         8, nil, 0x04)
f.oa_flag_stop    = ProtoField.bool  ("dz_dob.oa.flag_stop",           "Stop",           8, nil, 0x08)
f.oa_flag_twap    = ProtoField.bool  ("dz_dob.oa.flag_twap_child",     "TWAP Child",     8, nil, 0x10)
f.oa_pi_seq       = ProtoField.uint32("dz_dob.oa.per_instrument_seq",  "Per-Instrument Seq", base.DEC)
f.oa_order_id     = ProtoField.uint64("dz_dob.oa.order_id",            "Order ID",           base.HEX)
f.oa_enter_ts     = ProtoField.uint64("dz_dob.oa.enter_timestamp_ns",  "Enter Timestamp",    base.DEC)
f.oa_price        = ProtoField.int64 ("dz_dob.oa.price",               "Price")
f.oa_quantity     = ProtoField.uint64("dz_dob.oa.quantity",            "Quantity")

-- OrderCancel (0x11)
f.oc_inst_id      = ProtoField.uint32("dz_dob.oc.instrument_id",      "Instrument ID",      base.DEC)
f.oc_source_id    = ProtoField.uint16("dz_dob.oc.source_id",          "Source ID",          base.DEC)
f.oc_reason       = ProtoField.uint8 ("dz_dob.oc.reason",             "Cancel Reason",      base.DEC,
                                      { [0]="Unknown", [1]="User Cancel", [2]="Venue Expire",
                                        [3]="Self Trade", [4]="Margin", [5]="Risk Limit",
                                        [6]="Sibling Filled", [255]="Other" })
f.oc_pi_seq       = ProtoField.uint32("dz_dob.oc.per_instrument_seq", "Per-Instrument Seq", base.DEC)
f.oc_order_id     = ProtoField.uint64("dz_dob.oc.order_id",           "Order ID",           base.HEX)
f.oc_timestamp    = ProtoField.uint64("dz_dob.oc.timestamp_ns",       "Timestamp",          base.DEC)

-- OrderExecute (0x12)
f.ox_inst_id      = ProtoField.uint32("dz_dob.ox.instrument_id",      "Instrument ID",      base.DEC)
f.ox_source_id    = ProtoField.uint16("dz_dob.ox.source_id",          "Source ID",          base.DEC)
f.ox_aggressor    = ProtoField.uint8 ("dz_dob.ox.aggressor_side",     "Aggressor Side",     base.DEC,
                                      { [0]="Unknown", [1]="Buy", [2]="Sell" })
f.ox_exec_flags   = ProtoField.uint8 ("dz_dob.ox.exec_flags",         "Exec Flags",         base.HEX)
f.ox_pi_seq       = ProtoField.uint32("dz_dob.ox.per_instrument_seq", "Per-Instrument Seq", base.DEC)
f.ox_order_id     = ProtoField.uint64("dz_dob.ox.order_id",           "Order ID",           base.HEX)
f.ox_trade_id     = ProtoField.uint64("dz_dob.ox.trade_id",           "Trade ID",           base.HEX)
f.ox_timestamp    = ProtoField.uint64("dz_dob.ox.timestamp_ns",       "Timestamp",          base.DEC)
f.ox_exec_price   = ProtoField.int64 ("dz_dob.ox.exec_price",         "Exec Price")
f.ox_exec_qty     = ProtoField.uint64("dz_dob.ox.exec_quantity",      "Exec Quantity")

-- BatchBoundary (0x13)
f.bb_channel_id   = ProtoField.uint8 ("dz_dob.bb.channel_id", "Channel ID", base.DEC)
f.bb_phase        = ProtoField.uint8 ("dz_dob.bb.phase",      "Phase",      base.DEC,
                                      { [0]="Open", [1]="Close" })
f.bb_batch_id     = ProtoField.uint64("dz_dob.bb.batch_id",   "Batch ID",   base.DEC)

-- InstrumentReset (0x14) — phase 2
f.ir_inst_id      = ProtoField.uint32("dz_dob.ir.instrument_id",  "Instrument ID",  base.DEC)
f.ir_source_id    = ProtoField.uint16("dz_dob.ir.source_id",      "Source ID",      base.DEC)
f.ir_reason       = ProtoField.uint8 ("dz_dob.ir.reason",         "Reason",         base.DEC,
                                      { [0]="Unspecified", [1]="Publisher Inconsistency",
                                        [2]="Venue Resync", [3]="Upstream Gap", [255]="Other" })
f.ir_reset_seq    = ProtoField.uint32("dz_dob.ir.reset_seq",      "Reset Seq",      base.DEC)
f.ir_timestamp    = ProtoField.uint64("dz_dob.ir.timestamp_ns",   "Timestamp",      base.DEC)

-- SnapshotBegin (0x20) — phase 2
f.sb_inst_id      = ProtoField.uint32("dz_dob.sb.instrument_id",  "Instrument ID",  base.DEC)
f.sb_source_id    = ProtoField.uint16("dz_dob.sb.source_id",      "Source ID",      base.DEC)
f.sb_reset_seq    = ProtoField.uint32("dz_dob.sb.reset_seq",      "Reset Seq",      base.DEC)
f.sb_timestamp    = ProtoField.uint64("dz_dob.sb.timestamp_ns",   "Timestamp",      base.DEC)
f.sb_order_count  = ProtoField.uint32("dz_dob.sb.order_count",    "Order Count",    base.DEC)
f.sb_level_count  = ProtoField.uint32("dz_dob.sb.level_count",    "Level Count",    base.DEC)

-- SnapshotOrder (0x21) — phase 2
f.so_inst_id      = ProtoField.uint32("dz_dob.so.instrument_id",  "Instrument ID",  base.DEC)
f.so_source_id    = ProtoField.uint16("dz_dob.so.source_id",      "Source ID",      base.DEC)
f.so_side         = ProtoField.uint8 ("dz_dob.so.side",           "Side",           base.DEC,
                                      { [0]="Bid", [1]="Ask" })
f.so_order_flags  = ProtoField.uint8 ("dz_dob.so.order_flags",    "Order Flags",    base.HEX)
f.so_pi_seq       = ProtoField.uint32("dz_dob.so.per_instrument_seq", "Per-Instrument Seq", base.DEC)
f.so_order_id     = ProtoField.uint64("dz_dob.so.order_id",       "Order ID",       base.HEX)
f.so_price        = ProtoField.int64 ("dz_dob.so.price",          "Price")
f.so_quantity     = ProtoField.uint64("dz_dob.so.quantity",       "Quantity")

-- SnapshotEnd (0x22) — phase 2
-- 20 bytes total: header(4) + inst_id(4) + source_id(2) + reserved(2) + reset_seq(4) + checksum(4)
f.se_inst_id      = ProtoField.uint32("dz_dob.se.instrument_id",  "Instrument ID",  base.DEC)
f.se_source_id    = ProtoField.uint16("dz_dob.se.source_id",      "Source ID",      base.DEC)
f.se_reset_seq    = ProtoField.uint32("dz_dob.se.reset_seq",      "Reset Seq",      base.DEC)
f.se_checksum     = ProtoField.uint32("dz_dob.se.checksum",       "Checksum",       base.HEX)

-- ============================================================================
-- Constants
-- ============================================================================

local MAGIC = 0x4444
local FRAME_HEADER_SIZE = 24
local APP_MSG_HEADER_SIZE = 4

local MSG_TYPE_HEARTBEAT        = 0x01
local MSG_TYPE_INSTRUMENT_DEF   = 0x02
local MSG_TYPE_TRADE            = 0x04
local MSG_TYPE_END_OF_SESSION   = 0x06
local MSG_TYPE_MANIFEST_SUMMARY = 0x07
local MSG_TYPE_ORDER_ADD        = 0x10
local MSG_TYPE_ORDER_CANCEL     = 0x11
local MSG_TYPE_ORDER_EXECUTE    = 0x12
local MSG_TYPE_BATCH_BOUNDARY   = 0x13
local MSG_TYPE_INSTRUMENT_RESET = 0x14
local MSG_TYPE_SNAPSHOT_BEGIN   = 0x20
local MSG_TYPE_SNAPSHOT_ORDER   = 0x21
local MSG_TYPE_SNAPSHOT_END     = 0x22

local MSG_TYPE_NAMES = {
    [0x01] = "Heartbeat",
    [0x02] = "InstrumentDefinition",
    [0x04] = "Trade",
    [0x06] = "EndOfSession",
    [0x07] = "ManifestSummary",
    [0x10] = "OrderAdd",
    [0x11] = "OrderCancel",
    [0x12] = "OrderExecute",
    [0x13] = "BatchBoundary",
    [0x14] = "InstrumentReset",
    [0x20] = "SnapshotBegin",
    [0x21] = "SnapshotOrder",
    [0x22] = "SnapshotEnd",
}

-- ============================================================================
-- Preferences
-- ============================================================================

dz.prefs.mktdata_port = Pref.uint("Mktdata UDP port", 6000, "UDP port for OrderAdd/Cancel/Execute/Heartbeat")
dz.prefs.refdata_port = Pref.uint("Refdata UDP port", 6001, "UDP port for InstrumentDefinition/ManifestSummary")

-- ============================================================================
-- Per-message dissectors
-- ============================================================================

local function dissect_heartbeat(body, subtree)
    -- [4]: channel_id  [5..8]: reserved  [8..16]: timestamp_ns
    subtree:add_le(f.hb_channel_id, body(4, 1))
    subtree:add_le(f.hb_timestamp,  body(8, 8))
    return "Heartbeat"
end

local function dissect_instrument_definition(body, subtree)
    -- [4..8]: inst_id  [8..24]: symbol  [24..32]: leg1  [32..40]: leg2
    -- [40]: asset_class  [41]: price_exp  [42]: qty_exp  [43]: market_model
    -- [44..52]: tick_size  [52..60]: lot_size  [60..68]: contract_value
    -- [68..76]: expiry  [76]: settle_type  [77]: price_bound  [78..80]: manifest_seq
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

local function dissect_trade(body, subtree)
    -- [4..8]: inst_id  [8..10]: source_id  [10]: aggressor  [11]: flags
    -- [12..20]: source_ts  [20..28]: price  [28..36]: qty
    -- [36..44]: trade_id  [44..52]: cumulative_volume
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

local function dissect_end_of_session(body, subtree)
    -- [4..12]: timestamp_ns
    subtree:add_le(f.eos_timestamp, body(4, 8))
    return "EndOfSession"
end

local function dissect_manifest_summary(body, subtree)
    -- [4]: channel_id  [5..8]: reserved  [8..10]: manifest_seq
    -- [10..12]: reserved  [12..16]: inst_count  [16..24]: timestamp_ns
    subtree:add_le(f.ms_channel_id,   body(4, 1))
    subtree:add_le(f.ms_manifest_seq, body(8, 2))
    subtree:add_le(f.ms_inst_count,   body(12, 4))
    subtree:add_le(f.ms_timestamp,    body(16, 8))
    local seq   = body(8, 2):le_uint()
    local count = body(12, 4):le_uint()
    return string.format("ManifestSummary seq=%d count=%d", seq, count)
end

local function dissect_order_add(body, subtree)
    -- [4..8]: inst_id  [8..10]: source_id  [10]: side  [11]: order_flags
    -- [12..16]: per_instrument_seq  [16..24]: order_id
    -- [24..32]: enter_timestamp_ns  [32..40]: price  [40..48]: quantity
    -- [48..52]: reserved
    subtree:add_le(f.oa_inst_id,   body(4, 4))
    subtree:add_le(f.oa_source_id, body(8, 2))
    subtree:add_le(f.oa_side,      body(10, 1))
    local flags_item = subtree:add_le(f.oa_order_flags, body(11, 1))
    flags_item:add_le(f.oa_flag_post,   body(11, 1))
    flags_item:add_le(f.oa_flag_reduce, body(11, 1))
    flags_item:add_le(f.oa_flag_hidden, body(11, 1))
    flags_item:add_le(f.oa_flag_stop,   body(11, 1))
    flags_item:add_le(f.oa_flag_twap,   body(11, 1))
    subtree:add_le(f.oa_pi_seq,    body(12, 4))
    subtree:add_le(f.oa_order_id,  body(16, 8))
    subtree:add_le(f.oa_enter_ts,  body(24, 8))
    subtree:add_le(f.oa_price,     body(32, 8))
    subtree:add_le(f.oa_quantity,  body(40, 8))
    local inst_id  = body(4, 4):le_uint()
    local order_id = body(16, 8):le_uint64()
    local side     = body(10, 1):uint()
    local side_str = side == 0 and "Bid" or "Ask"
    local px       = body(32, 8):le_int64()
    local qty      = body(40, 8):le_uint64()
    return string.format("OrderAdd inst=%d %s px=%s qty=%s oid=%s",
        inst_id, side_str, tostring(px), tostring(qty), tostring(order_id))
end

local function dissect_order_cancel(body, subtree)
    -- [4..8]: inst_id  [8..10]: source_id  [10]: reason  [11]: reserved
    -- [12..16]: per_instrument_seq  [16..24]: order_id  [24..32]: timestamp_ns
    subtree:add_le(f.oc_inst_id,   body(4, 4))
    subtree:add_le(f.oc_source_id, body(8, 2))
    subtree:add_le(f.oc_reason,    body(10, 1))
    subtree:add_le(f.oc_pi_seq,    body(12, 4))
    subtree:add_le(f.oc_order_id,  body(16, 8))
    subtree:add_le(f.oc_timestamp, body(24, 8))
    local inst_id  = body(4, 4):le_uint()
    local order_id = body(16, 8):le_uint64()
    return string.format("OrderCancel inst=%d oid=%s", inst_id, tostring(order_id))
end

local function dissect_order_execute(body, subtree)
    -- [4..8]: inst_id  [8..10]: source_id  [10]: aggressor  [11]: exec_flags
    -- [12..16]: per_instrument_seq  [16..24]: order_id  [24..32]: trade_id
    -- [32..40]: timestamp_ns  [40..48]: exec_price  [48..56]: exec_quantity
    subtree:add_le(f.ox_inst_id,   body(4, 4))
    subtree:add_le(f.ox_source_id, body(8, 2))
    subtree:add_le(f.ox_aggressor, body(10, 1))
    subtree:add_le(f.ox_exec_flags,body(11, 1))
    subtree:add_le(f.ox_pi_seq,    body(12, 4))
    subtree:add_le(f.ox_order_id,  body(16, 8))
    subtree:add_le(f.ox_trade_id,  body(24, 8))
    subtree:add_le(f.ox_timestamp, body(32, 8))
    subtree:add_le(f.ox_exec_price,body(40, 8))
    subtree:add_le(f.ox_exec_qty,  body(48, 8))
    local inst_id  = body(4, 4):le_uint()
    local order_id = body(16, 8):le_uint64()
    local px       = body(40, 8):le_int64()
    local qty      = body(48, 8):le_uint64()
    return string.format("OrderExecute inst=%d oid=%s px=%s qty=%s",
        inst_id, tostring(order_id), tostring(px), tostring(qty))
end

local function dissect_batch_boundary(body, subtree)
    -- [4]: channel_id  [5]: phase  [6..8]: reserved  [8..16]: batch_id
    subtree:add_le(f.bb_channel_id, body(4, 1))
    subtree:add_le(f.bb_phase,      body(5, 1))
    subtree:add_le(f.bb_batch_id,   body(8, 8))
    local phase    = body(5, 1):uint()
    local batch_id = body(8, 8):le_uint64()
    local phase_str = phase == 0 and "Open" or "Close"
    return string.format("BatchBoundary %s batch=%s", phase_str, tostring(batch_id))
end

local function dissect_instrument_reset(body, subtree)
    -- Phase 2 message — layout may differ from this placeholder.
    -- [4..8]: inst_id  [8..10]: source_id  [10]: reason  [11]: reserved
    -- [12..16]: reset_seq  [16..20]: reserved  [20..28]: timestamp_ns
    subtree:add_le(f.ir_inst_id,   body(4, 4))
    subtree:add_le(f.ir_source_id, body(8, 2))
    subtree:add_le(f.ir_reason,    body(10, 1))
    subtree:add_le(f.ir_reset_seq, body(12, 4))
    subtree:add_le(f.ir_timestamp, body(20, 8))
    local inst_id = body(4, 4):le_uint()
    return string.format("InstrumentReset inst=%d", inst_id)
end

local function dissect_snapshot_begin(body, subtree)
    -- Phase 2 message.
    -- [4..8]: inst_id  [8..10]: source_id  [10..12]: reserved  [12..16]: reset_seq
    -- [16..24]: timestamp_ns  [24..28]: order_count  [28..32]: level_count  [32..36]: reserved
    subtree:add_le(f.sb_inst_id,     body(4, 4))
    subtree:add_le(f.sb_source_id,   body(8, 2))
    subtree:add_le(f.sb_reset_seq,   body(12, 4))
    subtree:add_le(f.sb_timestamp,   body(16, 8))
    subtree:add_le(f.sb_order_count, body(24, 4))
    subtree:add_le(f.sb_level_count, body(28, 4))
    local inst_id = body(4, 4):le_uint()
    return string.format("SnapshotBegin inst=%d", inst_id)
end

local function dissect_snapshot_order(body, subtree)
    -- Phase 2 message.
    -- [4..8]: inst_id  [8..10]: source_id  [10]: side  [11]: order_flags
    -- [12..16]: per_instrument_seq  [16..24]: order_id
    -- [24..32]: price  [32..40]: quantity  [40..44]: reserved
    subtree:add_le(f.so_inst_id,   body(4, 4))
    subtree:add_le(f.so_source_id, body(8, 2))
    subtree:add_le(f.so_side,      body(10, 1))
    subtree:add_le(f.so_order_flags, body(11, 1))
    subtree:add_le(f.so_pi_seq,    body(12, 4))
    subtree:add_le(f.so_order_id,  body(16, 8))
    subtree:add_le(f.so_price,     body(24, 8))
    subtree:add_le(f.so_quantity,  body(32, 8))
    local inst_id  = body(4, 4):le_uint()
    local order_id = body(16, 8):le_uint64()
    return string.format("SnapshotOrder inst=%d oid=%s", inst_id, tostring(order_id))
end

local function dissect_snapshot_end(body, subtree)
    -- Phase 2 message. 20 bytes total.
    -- [4..8]: inst_id  [8..10]: source_id  [10..12]: reserved
    -- [12..16]: reset_seq  [16..20]: checksum
    subtree:add_le(f.se_inst_id,   body(4, 4))
    subtree:add_le(f.se_source_id, body(8, 2))
    subtree:add_le(f.se_reset_seq, body(12, 4))
    subtree:add_le(f.se_checksum,  body(16, 4))
    local inst_id = body(4, 4):le_uint()
    return string.format("SnapshotEnd inst=%d", inst_id)
end

local DISSECTORS = {
    [MSG_TYPE_HEARTBEAT]        = dissect_heartbeat,
    [MSG_TYPE_INSTRUMENT_DEF]   = dissect_instrument_definition,
    [MSG_TYPE_TRADE]            = dissect_trade,
    [MSG_TYPE_END_OF_SESSION]   = dissect_end_of_session,
    [MSG_TYPE_MANIFEST_SUMMARY] = dissect_manifest_summary,
    [MSG_TYPE_ORDER_ADD]        = dissect_order_add,
    [MSG_TYPE_ORDER_CANCEL]     = dissect_order_cancel,
    [MSG_TYPE_ORDER_EXECUTE]    = dissect_order_execute,
    [MSG_TYPE_BATCH_BOUNDARY]   = dissect_batch_boundary,
    [MSG_TYPE_INSTRUMENT_RESET] = dissect_instrument_reset,
    [MSG_TYPE_SNAPSHOT_BEGIN]   = dissect_snapshot_begin,
    [MSG_TYPE_SNAPSHOT_ORDER]   = dissect_snapshot_order,
    [MSG_TYPE_SNAPSHOT_END]     = dissect_snapshot_end,
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

    pinfo.cols.protocol = "DZ-DoB"

    local root = tree:add(dz, tvb(), "DoubleZero Depth-of-Book Frame")

    -- Frame header
    local hdr = root:add(dz, tvb(0, FRAME_HEADER_SIZE), "Frame Header")
    hdr:add_le(f.magic,          tvb(0, 2))
    hdr:add_le(f.schema_version, tvb(2, 1))
    hdr:add_le(f.channel_id,     tvb(3, 1))
    hdr:add_le(f.sequence,       tvb(4, 8))
    hdr:add_le(f.send_timestamp, tvb(12, 8))
    hdr:add_le(f.msg_count,      tvb(20, 1))
    hdr:add_le(f.reset_count,    tvb(21, 1))
    hdr:add_le(f.frame_length,   tvb(22, 2))

    local channel_id = tvb(3, 1):uint()
    local seq        = tvb(4, 8):le_uint64()
    local msg_count  = tvb(20, 1):uint()

    local info_parts = {}
    table.insert(info_parts, string.format("ch=%d seq=%s msgs=%d", channel_id, tostring(seq), msg_count))

    -- Iterate application messages
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

        -- App message header fields
        msg_tree:add_le(f.msg_type,   tvb(offset,     1))
        msg_tree:add_le(f.msg_length, tvb(offset + 1, 1))
        local flags_item = msg_tree:add_le(f.msg_flags, tvb(offset + 2, 2))
        flags_item:add_le(f.msg_flag_snap, tvb(offset + 2, 2))

        -- Per-type body dissection
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
-- Register on configured ports
-- ============================================================================

local udp_table = DissectorTable.get("udp.port")
local registered_ports = {}

local function register_ports()
    for port, _ in pairs(registered_ports) do
        udp_table:remove(port, dz)
    end
    registered_ports = {}

    if dz.prefs.mktdata_port > 0 then
        udp_table:add(dz.prefs.mktdata_port, dz)
        registered_ports[dz.prefs.mktdata_port] = true
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
