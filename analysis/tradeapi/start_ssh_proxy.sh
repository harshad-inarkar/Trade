echo "Kill SSH Process"
pkill -f "ssh -i ~/.ssh/oracle_dhan.key"
kill -15 $(lsof -t -i:9090)

echo "Start Ssh Proxy"
ssh -i ~/.ssh/oracle_dhan.key -D 9090 ubuntu@80.225.247.216 -N -f