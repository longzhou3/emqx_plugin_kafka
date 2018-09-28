{application, emqx_plugin_kafka, [
	{description, "EMQ X Plugin Kafka"},
	{vsn, "3.0"},
	{id, ""},
	{modules, ['emqx_plugin_kafka','emqx_plugin_kafka_app','emqx_plugin_kafka_sup']},
	{registered, [emqx_plugin_kafka_sup]},
	{applications, [kernel,stdlib,ekaf]},
	{mod, {emqx_plugin_kafka_app, []}}
]}.