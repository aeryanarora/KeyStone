#!/usr/bin/env bash
set -u
cd /home/claude/distributed-kv/build

pkill -9 -f kvdb-server 2>/dev/null
sleep 0.3
rm -rf /tmp/raft1 /tmp/raft2 /tmp/raft3 /tmp/raft*.log

PEERS="1=127.0.0.1:18001,2=127.0.0.1:18002,3=127.0.0.1:18003"
COMMON="--peers=$PEERS --log-level=warn --election-timeout-min-ms=150 --election-timeout-max-ms=300 --heartbeat-ms=50"
CLI="./kvdb-cli"
EPS="127.0.0.1:18001,127.0.0.1:18002,127.0.0.1:18003"

start_node() {
  local id=$1 port=$2
  ./kvdb-server --node-id="$id" --port="$port" --data-dir="/tmp/raft$id" $COMMON > "/tmp/raft$id.log" 2>&1 &
  echo $! > "/tmp/raft$id.pid"
}

find_leader_port() {
  for p in 18001 18002 18003; do
    role=$(timeout 5 $CLI --endpoints="127.0.0.1:$p" status 2>/dev/null | grep -o "role=[a-z]*")
    if [ "$role" = "role=leader" ]; then
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

LEADER_PORT=$(find_leader_port)
echo "### leader is on port: $LEADER_PORT ###"
LEADER_ID=$((LEADER_PORT - 18000))
LEADER_PID=$(cat "/tmp/raft$LEADER_ID.pid")
echo "### killing node $LEADER_ID (pid $LEADER_PID) ###"
kill -9 "$LEADER_PID"
sleep 2

REMAINING=""
for p in 18001 18002 18003; do
  if [ "$p" != "$LEADER_PORT" ]; then
    if [ -z "$REMAINING" ]; then REMAINING="127.0.0.1:$p"; else REMAINING="$REMAINING,127.0.0.1:$p"; fi
  fi
done
echo "### remaining endpoints: $REMAINING ###"

echo "### status of survivors after leader kill ###"
for p in 18001 18002 18003; do
  if [ "$p" != "$LEADER_PORT" ]; then
    timeout 5 $CLI --endpoints="127.0.0.1:$p" status
  fi
done

echo "### write/read after failover ###"
timeout 10 $CLI --endpoints=$REMAINING put delta 4
timeout 10 $CLI --endpoints=$REMAINING get delta
timeout 10 $CLI --endpoints=$REMAINING get alpha
timeout 10 $CLI --endpoints=$REMAINING get gamma

echo "### restarting the killed node ###"
start_node "$LEADER_ID" "$LEADER_PORT"
sleep 2
timeout 5 $CLI --endpoints="127.0.0.1:$LEADER_PORT" status
echo "### reading delta directly from rejoined node ###"
timeout 5 $CLI --endpoints="127.0.0.1:$LEADER_PORT" get delta
echo "### reading gamma directly from rejoined node ###"
timeout 5 $CLI --endpoints="127.0.0.1:$LEADER_PORT" get gamma

echo "### ALL DONE ###"
pkill -9 -f kvdb-server 2>/dev/null
echo "### cleaned up ###"
