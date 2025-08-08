# libfabric_tcp_rxm_reproducer

1. Build the app with:
```
./build.sh
```
2. Run the app WITHOUT RXM utility provider:
```
./build/reproducer 128 0
```
   The result should be sth like:
```
INPUTS:
number of client processes = 128
use RXM utility provider = 0

serverExchangeAddresses DONE
```
3. Run the app WITH RXM utility provider:
```
./build/reproducer 128 1
```
   The result should be sth like:
```
INPUTS:
number of client processes = 128
use RXM utility provider = 1

serverExchangeAddresses DONE
ERR: Mismatch at iter:0, client:24. Expected: 24, got:559.
ERR: Mismatch at iter:0, client:25. Expected: 25, got:63.
ERR: Mismatch at iter:0, client:54. Expected: 54, got:711.
ERR: Mismatch at iter:0, client:81. Expected: 81, got:198.
ERR: Mismatch at iter:0, client:82. Expected: 82, got:254.
ERR: Mismatch at iter:0, client:110. Expected: 110, got:248.
ERR: Mismatch at iter:1, client:24. Expected: 25, got:560.
ERR: Mismatch at iter:1, client:25. Expected: 26, got:25.
ERR: Mismatch at iter:1, client:54. Expected: 55, got:712.
ERR: Mismatch at iter:1, client:81. Expected: 82, got:199.
ERR: Mismatch at iter:1, client:82. Expected: 83, got:82.
ERR: Mismatch at iter:1, client:110. Expected: 111, got:249.
(...)
```

In case the above mismatches don't show place, it is recommended to raise the number of clients and/or limit the number of cores used by the process, e.g.:
```
taskset -c 0-3 ./build/reproducer 256 1
```
