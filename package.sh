RABBITMQ_PLUGIN_DIR=/cygdrive/e/target/Picasso_BugFix/rabbitmq_server/rabbitmq_server-2.8.2/plugins

pushd ..;
zip erlzmq.ez erlzmq/ebin/* erlzmq/priv/*
cp erlzmq.ez $RABBITMQ_PLUGIN_DIR
popd
