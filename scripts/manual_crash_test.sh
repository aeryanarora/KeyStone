#!/usr/bin/env bash
set -u

if [ ! -x "./kvdb-server" ] || [ ! -x "./kvdb-cli" ]; then
  echo "error: run this from your build directory (the one with kvdb-server and kvdb-cli in it)"
  echo "  cd build && ../scripts/manual_crash_test.sh"
  exit 1
fi

pkill -9 -f kvdb-server 2>/dev/null
sleep 0.3
rm -rf /tmp/crashtest
CLI="./kvdb-cli"

echo "### starting standalone node ###"
./kvdb-server --node-id=1 --port=19001 --data-dir=/tmp/crashtest --log-level=warn --sync-writes=true &
PID=$!
sleep 1

echo "### writing 200 keys ###"
for i in $(seq 1 200); do
  timeout 5 $CLI --endpoints=127.0.0.1:19001 put "key$i" "value$i" > /dev/null
done
timeout 5 $CLI --endpoints=127.0.0.1:19001 get key199
timeout 5 $CLI --endpoints=127.0.0.1:19001 status

echo "### triggering admin crash (SimulateCrash + _Exit) ###"
timeout 5 $CLI --endpoints=127.0.0.1:19001 admin crash
sleep 1
pgrep -af "port=19001" || echo "process is gone, as expected"

echo "### restarting node, expecting full WAL-based recovery ###"
./kvdb-server --node-id=1 --port=19001 --data-dir=/tmp/crashtest --log-level=info &
PID2=$!
sleep 1

echo "### verifying all 200 keys survived the crash ###"
MISSING=0
for i in $(seq 1 200); do
  val=$(timeout 5 $CLI --endpoints=127.0.0.1:19001 get "key$i")
  if [ "$val" != "value$i" ]; then
    echo "MISMATCH: key$i -> got '$val', want 'value$i'"
    MISSING=$((MISSING+1))
  fi
done
echo "missing/mismatched keys: $MISSING / 200"
timeout 5 $CLI --endpoints=127.0.0.1:19001 status

kill -9 $PID2 2>/dev/null
echo "### DONE ###"
