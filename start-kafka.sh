export RABBITMQ_ERLANG_COOKIE=_rabbitcookie
export RABBITMQ_DEFAULT_USER=guest
export RABBITMQ_DEFAULT_PASS=guest

docker-compose -f docker-compose.yml up -d