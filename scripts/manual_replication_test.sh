#!/usr/bin/env bash
set -u

if [ ! -x "./kvdb-server" ] || [ ! -x "./kvdb-cli" ]; then
  echo "error: run this from your build directory (the one with kvdb-server and kvdb-cli in it)"
  echo "  cd build && ../scripts/manual_replication_test.sh"
  exit 1
fi

pkill -9 -f kvdb-server 2>/dev/null
sleep 0.3
rm -rf /tmp/repl1 /tmp/repl2 /tmp/repl3 /tmp/repl*.log

PEERS="1=127.0.0.1:18001,2=127.0.0.1:18002,3=127.0.0.1:18003"
COMMON="--peers=$PEERS --log-level=warn --heartbeat-ms=50 --heartbeat-timeout-ms=200"
CLI="./kvdb-cli"
EPS="127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003"

start_node() {
  local id=$1 port=$2
  ./kvdb-server --node-id="$id" --port="$port" --data-dir="/tmp/repl$id" $COMMON > "/tmp/repl$id.log" 2>&1 &
  echo $! > "/tmp/repl$id.pid"
}

find_primary_port() {
  for p in 18001 18002 18003; do
    role=$(timeout 5 $CLI --endpoints="127.0.0.1:$p" status 2>/dev/null | grep -o "role=[a-z]*")
    if [ "$role" = "role=primary" ]; then
      echo "$p"
      return 0
    fi
  done
  echo ""
}

echo "### starting 3-node cluster ###"
start_node 1 18001
start_node 2 18002
start_node 3 18003
sleep 2
pgrep -af kvdb-server

echo "### initial writes ###"
timeout 10 $CLI --endpoints=$EPS put alpha 1
timeout 10 $CLI --endpoints=$EPS put beta 2
timeout 10 $CLI --endpoints=$EPS put gamma 3
timeout 5 $CLI --endpoints=$EPS get alpha

PRIMARY_PORT=$(find_primary_port)
echo "### primary is on port: $PRIMARY_PORT ###"
PRIMARY_ID=$((PRIMARY_PORT - 18000))
PRIMARY_PID=$(cat "/tmp/repl$PRIMARY_ID.pid")
echo "### killing node $PRIMARY_ID (pid $PRIMARY_PID) ###"
kill -9 "$PRIMARY_PID"
sleep 2

REMAINING=""
for p in 18001 18002 18003; do
  if [ "$p" != "$PRIMARY_PORT" ]; then
    if [ -z "$REMAINING" ]; then REMAINING="127.0.0.1:$p"; else REMAINING="$REMAINING,127.0.0.1:$p"; fi
  fi
done
echo "### remaining endpoints: $REMAINING ###"

echo "### status of survivors after primary kill ###"
for p in 18001 18002 18003; do
  if [ "$p" != "$PRIMARY_PORT" ]; then
    timeout 5 $CLI --endpoints="127.0.0.1:$p" status
  fi
done

echo "### write/read after promotion ###"
timeout 10 $CLI --endpoints=$REMAINING put delta 4
timeout 10 $CLI --endpoints=$REMAINING get delta
timeout 10 $CLI --endpoints=$REMAINING get alpha
timeout 10 $CLI --endpoints=$REMAINING get gamma

echo "### restarting the killed node (rejoins as backup, fences off if it ever tries to act as primary) ###"
start_node "$PRIMARY_ID" "$PRIMARY_PORT"
sleep 2
timeout 5 $CLI --endpoints="127.0.0.1:$PRIMARY_PORT" status
echo "### reading delta directly from rejoined node (should have caught up) ###"
timeout 5 $CLI --endpoints="127.0.0.1:$PRIMARY_PORT" get delta
echo "### reading gamma directly from rejoined node ###"
timeout 5 $CLI --endpoints="127.0.0.1:$PRIMARY_PORT" get gamma

echo "### ALL DONE ###"
pkill -9 -f kvdb-server 2>/dev/null
echo "### cleaned up ###"
