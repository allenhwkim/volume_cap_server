-include("include/debug.hrl").
-record(vc_meta, {
	'id',
	'start',
	'end',
	'interval',
	'total'}).

-record(vc_interval, {
	'id',
	'dc_id',
	'meta_id',
	'start',
	'end',
	'interval',
	'total',
	'available',
	'consumed'=0}).

-record(volumecap, {
	'id',
	'meta_id',
	'start',
	'end',
	'interval',
	'total',
	'available',
	'consumed'}).
