goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/api
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkHTTP_GetInitialSCIDCode/size=256-24         	   10954	     75193 ns/op	   3.40 MB/s	    9322 B/op	     100 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=256-24         	    9435	     62213 ns/op	   4.11 MB/s	    9292 B/op	     100 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=256-24         	    9294	    132628 ns/op	   1.93 MB/s	    9295 B/op	     100 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=2048-24        	    2853	    187735 ns/op	  10.91 MB/s	   10836 B/op	     102 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=2048-24        	    2986	    191527 ns/op	  10.69 MB/s	   10934 B/op	     102 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=2048-24        	    3207	    177148 ns/op	  11.56 MB/s	   10866 B/op	     102 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=16384-24       	    1714	    294594 ns/op	  55.62 MB/s	   27079 B/op	     103 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=16384-24       	    1803	    306611 ns/op	  53.44 MB/s	   27634 B/op	     104 allocs/op
BenchmarkHTTP_GetInitialSCIDCode/size=16384-24       	    2671	    299889 ns/op	  54.63 MB/s	   27452 B/op	     104 allocs/op
BenchmarkWS_GetInitialSCIDCode_Dispatch-24           	   84646	      7163 ns/op	    3376 B/op	      28 allocs/op
BenchmarkWS_GetInitialSCIDCode_Dispatch-24           	   94348	      6709 ns/op	    3376 B/op	      28 allocs/op
BenchmarkWS_GetInitialSCIDCode_Dispatch-24           	   82040	      7241 ns/op	    3376 B/op	      28 allocs/op
BenchmarkTELAContentCache_Get/fill=64-24             	17424232	        37.44 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=64-24             	17441707	        36.29 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=64-24             	17020310	        35.45 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=1024-24           	17281702	        35.56 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=1024-24           	17187543	        34.25 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=1024-24           	16057764	        36.05 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=8192-24           	18507093	        36.89 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=8192-24           	16469896	        37.14 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Get/fill=8192-24           	19676388	        33.48 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_Put/no-evict-24            	 1000000	       788.8 ns/op	     223 B/op	       4 allocs/op
BenchmarkTELAContentCache_Put/no-evict-24            	 1000000	       729.2 ns/op	     223 B/op	       4 allocs/op
BenchmarkTELAContentCache_Put/no-evict-24            	 1000000	       700.6 ns/op	     223 B/op	       4 allocs/op
BenchmarkTELAContentCache_Put/with-evict-24          	 1552236	       406.0 ns/op	     120 B/op	       4 allocs/op
BenchmarkTELAContentCache_Put/with-evict-24          	 1546284	       382.9 ns/op	     120 B/op	       4 allocs/op
BenchmarkTELAContentCache_Put/with-evict-24          	 1551049	       397.8 ns/op	     120 B/op	       4 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=64-24         	  606415	      1013 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=64-24         	  581586	       972.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=64-24         	  587916	      1001 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=1024-24       	  563142	      1081 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=1024-24       	  576103	      1063 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=1024-24       	  594529	      1040 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=8192-24       	  616984	      1038 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=8192-24       	  567340	      1232 ns/op	       0 B/op	       0 allocs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=8192-24       	  520789	      1059 ns/op	       0 B/op	       0 allocs/op
BenchmarkDecompressTELAGzip-24                                	   18366	     34271 ns/op	   2.33 MB/s	   53026 B/op	      17 allocs/op
BenchmarkDecompressTELAGzip-24                                	   19225	     32180 ns/op	   2.49 MB/s	   53026 B/op	      17 allocs/op
BenchmarkDecompressTELAGzip-24                                	   19087	     30634 ns/op	   2.61 MB/s	   53026 B/op	      17 allocs/op
BenchmarkExtractDOCBodyFromSource-24                          	   62470	      9239 ns/op	 380.36 MB/s	    3456 B/op	       1 allocs/op
BenchmarkExtractDOCBodyFromSource-24                          	   69074	      8789 ns/op	 399.83 MB/s	    3456 B/op	       1 allocs/op
BenchmarkExtractDOCBodyFromSource-24                          	   70540	      8825 ns/op	 398.19 MB/s	    3456 B/op	       1 allocs/op
BenchmarkExtractDocShardBodyFromSource-24                     	  302790	      2543 ns/op	1834.50 MB/s	    4864 B/op	       1 allocs/op
BenchmarkExtractDocShardBodyFromSource-24                     	  267673	      2588 ns/op	1802.22 MB/s	    4864 B/op	       1 allocs/op
BenchmarkExtractDocShardBodyFromSource-24                     	  408942	      1817 ns/op	2567.18 MB/s	    4864 B/op	       1 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Printable-24               	 6323350	        97.49 ns/op	      16 B/op	       1 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Printable-24               	 6032460	        95.26 ns/op	      16 B/op	       1 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Printable-24               	 6166260	       102.0 ns/op	      16 B/op	       1 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Passthrough-24             	 3777728	       174.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Passthrough-24             	 3377893	       175.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkDecodeHexIfPrintableASCII_Passthrough-24             	 3273745	       176.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkReadTELASigFields_Signed-24                          	 1630514	       345.6 ns/op	      16 B/op	       1 allocs/op
BenchmarkReadTELASigFields_Signed-24                          	 1705132	       353.0 ns/op	      16 B/op	       1 allocs/op
BenchmarkReadTELASigFields_Signed-24                          	 1711681	       354.2 ns/op	      16 B/op	       1 allocs/op
BenchmarkReadTELASigFields_Unsigned-24                        	10302002	        55.00 ns/op	       0 B/op	       0 allocs/op
BenchmarkReadTELASigFields_Unsigned-24                        	10900548	        55.53 ns/op	       0 B/op	       0 allocs/op
BenchmarkReadTELASigFields_Unsigned-24                        	12033139	        54.96 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/api	472.676s
?   	github.com/hypergnomon/hypergnomon/cmd/benchvs	[no test files]
?   	github.com/hypergnomon/hypergnomon/cmd/hypergnomon	[no test files]
?   	github.com/hypergnomon/hypergnomon/cmd/verify-classmeta-tags	[no test files]
?   	github.com/hypergnomon/hypergnomon/cmd/wstest	[no test files]
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/eventbus
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkFilter_Match_Speculative/speculative_event_opted_out-24         	231805669	         2.504 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_out-24         	233964921	         2.521 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_out-24         	240413510	         2.609 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_in-24          	219382437	         6.798 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_in-24          	76887589	         9.098 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_in-24          	73891624	         9.417 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_default_filter-24      	72501418	         8.241 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_default_filter-24      	81564958	         8.231 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_default_filter-24      	71662326	         8.102 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_with_filters-24        	24534357	        26.65 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_with_filters-24        	23903978	        27.06 ns/op	       0 B/op	       0 allocs/op
BenchmarkFilter_Match_Speculative/finalized_event_with_filters-24        	23154133	        26.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkBus_PublishFanOut_Speculative-24                                	26504810	        21.17 ns/op	       0 B/op	       0 allocs/op
BenchmarkBus_PublishFanOut_Speculative-24                                	28307227	        21.05 ns/op	       0 B/op	       0 allocs/op
BenchmarkBus_PublishFanOut_Speculative-24                                	25992817	        21.49 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/eventbus	12.240s
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/indexer
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkClassifySC_TELAIndex-24                     	  293918	      2076 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_TELAIndex-24                     	  299576	      2088 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_TELAIndex-24                     	  283227	      2117 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_TELADoc-24                       	  172449	      3644 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_TELADoc-24                       	  154858	      4899 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_TELADoc-24                       	   86676	      6879 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_G45NFA-24                        	 1000000	       507.5 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_G45NFA-24                        	 1000000	       530.9 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_G45NFA-24                        	  981932	       612.8 ns/op	     112 B/op	       5 allocs/op
BenchmarkClassifySC_Miss-24                          	   39120	     14958 ns/op	      80 B/op	       4 allocs/op
BenchmarkClassifySC_Miss-24                          	   42973	     13246 ns/op	      80 B/op	       4 allocs/op
BenchmarkClassifySC_Miss-24                          	   38787	     14077 ns/op	      80 B/op	       4 allocs/op
BenchmarkGetSCCode_CacheHit/size=256-24              	  519138	      1267 ns/op	    1048 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=256-24              	  491412	      1327 ns/op	    1048 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=256-24              	  414724	      1310 ns/op	    1048 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=2048-24             	  255979	      2133 ns/op	    2504 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=2048-24             	  276674	      2078 ns/op	    2504 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=2048-24             	  275316	      2139 ns/op	    2504 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=16384-24            	   83713	      8448 ns/op	   16840 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=16384-24            	   65781	      8010 ns/op	   16840 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheHit/size=16384-24            	   70497	      7714 ns/op	   16840 B/op	       9 allocs/op
BenchmarkGetSCCode_CacheMiss-24                      	    5559	    123944 ns/op	   43760 B/op	      94 allocs/op
BenchmarkGetSCCode_CacheMiss-24                      	    6136	    122885 ns/op	   43835 B/op	      94 allocs/op
BenchmarkGetSCCode_CacheMiss-24                      	    5772	    129555 ns/op	   43807 B/op	      95 allocs/op
BenchmarkGetSCCode_CacheMiss_WithLatency-24          	     369	   1593285 ns/op	   36506 B/op	      89 allocs/op
BenchmarkGetSCCode_CacheMiss_WithLatency-24          	     379	   1599796 ns/op	   36808 B/op	      90 allocs/op
BenchmarkGetSCCode_CacheMiss_WithLatency-24          	     378	   1618399 ns/op	   36673 B/op	      89 allocs/op
BenchmarkGetSCCode_ConcurrentMiss_SingleFlight-24    	   33150	     21289 ns/op	    6667 B/op	      23 allocs/op
BenchmarkGetSCCode_ConcurrentMiss_SingleFlight-24    	   29704	     20961 ns/op	    6690 B/op	      23 allocs/op
BenchmarkGetSCCode_ConcurrentMiss_SingleFlight-24    	   32515	     21590 ns/op	    6688 B/op	      23 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/indexer	21.965s
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/pkg/gnomes
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkFacadeFieldRefresh-24    	82554795	         7.029 ns/op	       0 B/op	       0 allocs/op
BenchmarkFacadeFieldRefresh-24    	85216377	         7.142 ns/op	       0 B/op	       0 allocs/op
BenchmarkFacadeFieldRefresh-24    	85836909	         7.055 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/pkg/gnomes	2.329s
?   	github.com/hypergnomon/hypergnomon/pkg/gnomes/example	[no test files]
?   	github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer	[no test files]
?   	github.com/hypergnomon/hypergnomon/pkg/gnomes/storage	[no test files]
?   	github.com/hypergnomon/hypergnomon/pkg/gnomes/structures	[no test files]
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/pool
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkSCTXParse_Pool-24             	68666383	         8.933 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_Pool-24             	66301272	         9.574 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_Pool-24             	59659938	        10.29 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_New-24              	154705522	         3.784 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_New-24              	164796681	         4.029 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_New-24              	100000000	         5.195 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool-24              	34205965	        17.66 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool-24              	33391582	        18.21 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool-24              	34507150	        20.71 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_New-24               	  254067	      2358 ns/op	    5760 B/op	       2 allocs/op
BenchmarkWorkItem_New-24               	  261408	      2265 ns/op	    5760 B/op	       2 allocs/op
BenchmarkWorkItem_New-24               	  304501	      2335 ns/op	    5760 B/op	       2 allocs/op
BenchmarkInternSCID-24                 	 2077681	       284.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkInternSCID-24                 	 2146476	       285.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkInternSCID-24                 	 2090578	       282.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkInternSCID_NoIntern-24        	 2025434	       300.3 ns/op	     320 B/op	       5 allocs/op
BenchmarkInternSCID_NoIntern-24        	 2103678	       280.9 ns/op	     320 B/op	       5 allocs/op
BenchmarkInternSCID_NoIntern-24        	 2061940	       294.9 ns/op	     320 B/op	       5 allocs/op
BenchmarkSCTXParse_Pool_Parallel-24    	299562039	         2.461 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_Pool_Parallel-24    	274857291	         2.295 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_Pool_Parallel-24    	269834887	         2.213 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool_Parallel-24     	239207839	         2.471 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool_Parallel-24     	231373471	         2.539 ns/op	       0 B/op	       0 allocs/op
BenchmarkWorkItem_Pool_Parallel-24     	248414802	         2.551 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/pool	16.786s
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/rpc
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkPool_GetPut-24             	22018914	        27.07 ns/op	       0 B/op	       0 allocs/op
BenchmarkPool_GetPut-24             	20512258	        32.02 ns/op	       0 B/op	       0 allocs/op
BenchmarkPool_GetPut-24             	19098243	        30.52 ns/op	       0 B/op	       0 allocs/op
BenchmarkPool_GetPut_Parallel-24    	13603467	        44.34 ns/op	       0 B/op	       0 allocs/op
BenchmarkPool_GetPut_Parallel-24    	12627776	       158.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkPool_GetPut_Parallel-24    	 4236236	       286.3 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/rpc	6.947s
?   	github.com/hypergnomon/hypergnomon/rpc/rwc	[no test files]
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/storage
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkFlushBatch_AddrSCIDs-24                     	     552	   1173130 ns/op	      1000 addr_scid_pairs	  641971 B/op	   11076 allocs/op
BenchmarkFlushBatch_AddrSCIDs-24                     	     438	   1207871 ns/op	      1000 addr_scid_pairs	  642678 B/op	   11086 allocs/op
BenchmarkFlushBatch_AddrSCIDs-24                     	     536	   1278606 ns/op	      1000 addr_scid_pairs	  642171 B/op	   11077 allocs/op
BenchmarkFlushBatch_100-24                           	     402	   4193336 ns/op	       100.0 records/flush	 1743125 B/op	    8076 allocs/op
BenchmarkFlushBatch_100-24                           	     234	   3607115 ns/op	       100.0 records/flush	 1253564 B/op	    7973 allocs/op
BenchmarkFlushBatch_100-24                           	     223	   3918923 ns/op	       100.0 records/flush	 1212008 B/op	    7963 allocs/op
BenchmarkFlushBatch_1000-24                          	      25	  24649712 ns/op	      1000 records/flush	 9197978 B/op	   86202 allocs/op
BenchmarkFlushBatch_1000-24                          	      25	  24914896 ns/op	      1000 records/flush	 8920821 B/op	   86261 allocs/op
BenchmarkFlushBatch_1000-24                          	      22	  24405295 ns/op	      1000 records/flush	 9079526 B/op	   86661 allocs/op
BenchmarkFlushBatch_10000-24                         	       1	 800628200 ns/op	     10000 records/flush	326796016 B/op	 2478031 allocs/op
BenchmarkFlushBatch_10000-24                         	       1	 751038300 ns/op	     10000 records/flush	337892792 B/op	 2518565 allocs/op
BenchmarkFlushBatch_10000-24                         	       1	 787700400 ns/op	     10000 records/flush	325402752 B/op	 2478030 allocs/op
BenchmarkIndividualWrites-24                         	      24	  26390458 ns/op	       100.0 records/iter	 4784214 B/op	   30119 allocs/op
BenchmarkIndividualWrites-24                         	      21	  25731043 ns/op	       100.0 records/iter	 4884316 B/op	   30202 allocs/op
BenchmarkIndividualWrites-24                         	      21	  27039614 ns/op	       100.0 records/iter	 4996824 B/op	   30167 allocs/op
BenchmarkFlushBatch_Scaling/n=100-24                 	     194	   3911325 ns/op	       100.0 records/flush	 1148390 B/op	    7940 allocs/op
BenchmarkFlushBatch_Scaling/n=100-24                 	     196	   3824354 ns/op	       100.0 records/flush	 1153377 B/op	    7942 allocs/op
BenchmarkFlushBatch_Scaling/n=100-24                 	     188	   3987272 ns/op	       100.0 records/flush	 1133706 B/op	    7936 allocs/op
BenchmarkFlushBatch_Scaling/n=500-24                 	      48	  13096281 ns/op	       500.0 records/flush	 4359324 B/op	   41137 allocs/op
BenchmarkFlushBatch_Scaling/n=500-24                 	      48	  13467679 ns/op	       500.0 records/flush	 4498187 B/op	   41310 allocs/op
BenchmarkFlushBatch_Scaling/n=500-24                 	      45	  13007551 ns/op	       500.0 records/flush	 4321762 B/op	   41102 allocs/op
BenchmarkFlushBatch_Scaling/n=1000-24                	      22	  24046795 ns/op	      1000 records/flush	 8519519 B/op	   86490 allocs/op
BenchmarkFlushBatch_Scaling/n=1000-24                	      25	  26054012 ns/op	      1000 records/flush	 9363484 B/op	   86112 allocs/op
BenchmarkFlushBatch_Scaling/n=1000-24                	      25	  23465708 ns/op	      1000 records/flush	 8800399 B/op	   85775 allocs/op
BenchmarkFlushBatch_Scaling/n=5000-24                	       3	 181241467 ns/op	      5000 records/flush	81841578 B/op	  676244 allocs/op
BenchmarkFlushBatch_Scaling/n=5000-24                	       3	 181189400 ns/op	      5000 records/flush	81326330 B/op	  685134 allocs/op
BenchmarkFlushBatch_Scaling/n=5000-24                	       3	 189952433 ns/op	      5000 records/flush	81992298 B/op	  670600 allocs/op
BenchmarkFlushBatch_Scaling/n=10000-24               	       1	 789853500 ns/op	     10000 records/flush	317764808 B/op	 2518553 allocs/op
BenchmarkFlushBatch_Scaling/n=10000-24               	       1	 744572300 ns/op	     10000 records/flush	308913296 B/op	 2558676 allocs/op
BenchmarkFlushBatch_Scaling/n=10000-24               	       1	 760310900 ns/op	     10000 records/flush	343525992 B/op	 2518560 allocs/op
BenchmarkBatchAlloc/Pool_Pair-24                     	 6295854	        99.75 ns/op	       0 B/op	       0 allocs/op
BenchmarkBatchAlloc/Pool_Pair-24                     	 5858064	        98.39 ns/op	       0 B/op	       0 allocs/op
BenchmarkBatchAlloc/Pool_Pair-24                     	 5982172	       102.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkBatchAlloc/Direct_New-24                    	   63270	      9298 ns/op	   13720 B/op	      21 allocs/op
BenchmarkBatchAlloc/Direct_New-24                    	   69523	      9394 ns/op	   13720 B/op	      21 allocs/op
BenchmarkBatchAlloc/Direct_New-24                    	   76113	     10016 ns/op	   13720 B/op	      21 allocs/op
BenchmarkBboltStore_GetSCIDClass_Typed-24            	  530959	      1725 ns/op	    1032 B/op	      18 allocs/op
BenchmarkBboltStore_GetSCIDClass_Typed-24            	  344949	      1794 ns/op	    1032 B/op	      18 allocs/op
BenchmarkBboltStore_GetSCIDClass_Typed-24            	  329617	      1719 ns/op	    1032 B/op	      18 allocs/op
BenchmarkBboltStore_GetSCIDClass_LegacyMsgpack-24    	  216278	      3010 ns/op	    1145 B/op	      19 allocs/op
BenchmarkBboltStore_GetSCIDClass_LegacyMsgpack-24    	  189234	      2948 ns/op	    1145 B/op	      19 allocs/op
BenchmarkBboltStore_GetSCIDClass_LegacyMsgpack-24    	  183487	      2997 ns/op	    1145 B/op	      19 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=1-24             	   13503	     46842 ns/op	   12302 B/op	     128 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=1-24             	   13329	     45504 ns/op	   12300 B/op	     128 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=1-24             	   13135	     47718 ns/op	   12299 B/op	     128 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=16-24            	    5601	     99500 ns/op	   29315 B/op	     423 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=16-24            	    6176	     99298 ns/op	   29297 B/op	     423 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=16-24            	    6216	     99935 ns/op	   29298 B/op	     423 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=128-24           	     952	    608253 ns/op	  189220 B/op	    3102 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=128-24           	     913	    604343 ns/op	  189207 B/op	    3102 allocs/op
BenchmarkFlushBatch_WithClassMeta/n=128-24           	    1020	    587085 ns/op	  189110 B/op	    3101 allocs/op
BenchmarkGetSCIDsByOwner/n=1-24                      	  474176	      1261 ns/op	     640 B/op	      11 allocs/op
BenchmarkGetSCIDsByOwner/n=1-24                      	  435890	      1365 ns/op	     640 B/op	      11 allocs/op
BenchmarkGetSCIDsByOwner/n=1-24                      	  623091	      1320 ns/op	     640 B/op	      11 allocs/op
BenchmarkGetSCIDsByOwner/n=16-24                     	  158851	      4162 ns/op	    2080 B/op	      30 allocs/op
BenchmarkGetSCIDsByOwner/n=16-24                     	  140833	      4145 ns/op	    2080 B/op	      30 allocs/op
BenchmarkGetSCIDsByOwner/n=16-24                     	  165543	      4221 ns/op	    2080 B/op	      30 allocs/op
BenchmarkGetSCIDsByOwner/n=128-24                    	   25728	     24852 ns/op	   13216 B/op	     145 allocs/op
BenchmarkGetSCIDsByOwner/n=128-24                    	   22644	     23166 ns/op	   13216 B/op	     145 allocs/op
BenchmarkGetSCIDsByOwner/n=128-24                    	   29497	     23022 ns/op	   13216 B/op	     145 allocs/op
BenchmarkGetSCIDsByOwner/n=1024-24                   	    2912	    208016 ns/op	  125953 B/op	    1046 allocs/op
BenchmarkGetSCIDsByOwner/n=1024-24                   	    3573	    205291 ns/op	  125952 B/op	    1046 allocs/op
BenchmarkGetSCIDsByOwner/n=1024-24                   	    4141	    194559 ns/op	  125953 B/op	    1046 allocs/op
BenchmarkGetRatingsForSCID/raters=10-24              	   67760	      9047 ns/op	    4133 B/op	     109 allocs/op
BenchmarkGetRatingsForSCID/raters=10-24              	   69945	      8194 ns/op	    4133 B/op	     109 allocs/op
BenchmarkGetRatingsForSCID/raters=10-24              	   67264	      8321 ns/op	    4133 B/op	     109 allocs/op
BenchmarkGetRatingsForSCID/raters=100-24             	    9501	     61028 ns/op	   31260 B/op	     742 allocs/op
BenchmarkGetRatingsForSCID/raters=100-24             	   10000	     64002 ns/op	   31261 B/op	     742 allocs/op
BenchmarkGetRatingsForSCID/raters=100-24             	   10000	     63163 ns/op	   31260 B/op	     742 allocs/op
BenchmarkGetRatingsForSCID/raters=1000-24            	    1128	    561659 ns/op	  279872 B/op	    7046 allocs/op
BenchmarkGetRatingsForSCID/raters=1000-24            	    1125	    593487 ns/op	  279870 B/op	    7046 allocs/op
BenchmarkGetRatingsForSCID/raters=1000-24            	    1060	    547875 ns/op	  279876 B/op	    7046 allocs/op
BenchmarkGetRatingsForSCID_LatestHeightScan-24       	    9466	     63650 ns/op	   31854 B/op	     751 allocs/op
BenchmarkGetRatingsForSCID_LatestHeightScan-24       	   10000	     60595 ns/op	   31855 B/op	     751 allocs/op
BenchmarkGetRatingsForSCID_LatestHeightScan-24       	    7622	     70674 ns/op	   31853 B/op	     751 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=256-24          	 1000000	       645.1 ns/op	 396.83 MB/s	     400 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=256-24          	 1000000	       586.2 ns/op	 436.71 MB/s	     400 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=256-24          	 1000000	       589.1 ns/op	 434.53 MB/s	     400 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=2048-24         	  362079	      1663 ns/op	1231.48 MB/s	    2418 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=2048-24         	  327355	      1609 ns/op	1272.60 MB/s	    2418 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=2048-24         	  513284	      1785 ns/op	1147.55 MB/s	    2418 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=16384-24        	   61594	      9062 ns/op	1807.91 MB/s	   18555 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=16384-24        	   78274	      8619 ns/op	1900.89 MB/s	   18555 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=16384-24        	   62064	      8805 ns/op	1860.80 MB/s	   18555 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=131072-24       	   10000	     59435 ns/op	2205.30 MB/s	  139457 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=131072-24       	   10000	     68589 ns/op	1910.96 MB/s	  139470 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Msgpack/size=131072-24       	   10000	     51194 ns/op	2560.28 MB/s	  139457 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=256-24        	 1000000	       672.1 ns/op	 380.92 MB/s	     328 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=256-24        	  894760	       680.3 ns/op	 376.32 MB/s	     328 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=256-24        	  907893	       660.7 ns/op	 387.50 MB/s	     328 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=2048-24       	  289497	      2053 ns/op	 997.46 MB/s	    2122 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=2048-24       	  250096	      2183 ns/op	 938.02 MB/s	    2122 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=2048-24       	  430035	      1956 ns/op	1046.86 MB/s	    2122 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=16384-24      	   75036	      9610 ns/op	1704.87 MB/s	   16474 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=16384-24      	   68282	      8565 ns/op	1912.99 MB/s	   16472 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=16384-24      	   70753	     10030 ns/op	1633.44 MB/s	   16472 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=131072-24     	   12519	     45036 ns/op	2910.38 MB/s	  131330 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=131072-24     	   10278	     69130 ns/op	1896.01 MB/s	  131530 B/op	       3 allocs/op
BenchmarkSCCode_Unmarshal_Msgpack/size=131072-24     	   12951	     50009 ns/op	2620.98 MB/s	  131392 B/op	       3 allocs/op
BenchmarkSCCode_Marshal_Typed/size=256-24            	 3275089	       209.6 ns/op	1221.41 MB/s	     288 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=256-24            	 3579272	       195.1 ns/op	1312.41 MB/s	     288 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=256-24            	 3494725	       178.6 ns/op	1433.54 MB/s	     288 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=2048-24           	  726937	       968.4 ns/op	2114.78 MB/s	    2304 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=2048-24           	  631664	      1077 ns/op	1900.75 MB/s	    2304 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=2048-24           	  912574	       855.4 ns/op	2394.15 MB/s	    2304 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=16384-24          	   92156	      7268 ns/op	2254.40 MB/s	   18432 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=16384-24          	   87577	      7167 ns/op	2285.90 MB/s	   18432 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=16384-24          	  105238	      7839 ns/op	2090.17 MB/s	   18432 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=131072-24         	   12504	     44307 ns/op	2958.26 MB/s	  139275 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=131072-24         	   12717	     52892 ns/op	2478.09 MB/s	  139276 B/op	       1 allocs/op
BenchmarkSCCode_Marshal_Typed/size=131072-24         	   13674	     55787 ns/op	2349.53 MB/s	  139275 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=256-24          	 4938060	       153.3 ns/op	1669.93 MB/s	     256 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=256-24          	 3516963	       150.7 ns/op	1699.17 MB/s	     256 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=256-24          	 4020601	       163.2 ns/op	1568.58 MB/s	     256 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=2048-24         	  745434	      1052 ns/op	1946.39 MB/s	    2048 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=2048-24         	  606574	      1061 ns/op	1930.55 MB/s	    2048 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=2048-24         	 1000000	      1079 ns/op	1898.72 MB/s	    2048 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=16384-24        	   90788	      6210 ns/op	2638.41 MB/s	   16384 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=16384-24        	  115140	      6644 ns/op	2466.13 MB/s	   16384 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=16384-24        	  156374	      6263 ns/op	2616.15 MB/s	   16384 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=131072-24       	   17866	     33741 ns/op	3884.67 MB/s	  131072 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=131072-24       	   14842	     34911 ns/op	3754.49 MB/s	  131073 B/op	       1 allocs/op
BenchmarkSCCode_Unmarshal_Typed/size=131072-24       	   15507	     38920 ns/op	3367.77 MB/s	  131073 B/op	       1 allocs/op
BenchmarkBboltStore_PutSCCode/size=256-24            	   16243	     35220 ns/op	   7.27 MB/s	    8976 B/op	      48 allocs/op
BenchmarkBboltStore_PutSCCode/size=256-24            	   17016	     36830 ns/op	   6.95 MB/s	    8975 B/op	      48 allocs/op
BenchmarkBboltStore_PutSCCode/size=256-24            	   15819	     36862 ns/op	   6.94 MB/s	    8974 B/op	      48 allocs/op
BenchmarkBboltStore_PutSCCode/size=2048-24           	   13369	     51772 ns/op	  39.56 MB/s	   10379 B/op	      52 allocs/op
BenchmarkBboltStore_PutSCCode/size=2048-24           	   13846	     47535 ns/op	  43.08 MB/s	   10381 B/op	      52 allocs/op
BenchmarkBboltStore_PutSCCode/size=2048-24           	   12996	     43950 ns/op	  46.60 MB/s	   10381 B/op	      52 allocs/op
BenchmarkBboltStore_PutSCCode/size=16384-24          	    9806	     82869 ns/op	 197.71 MB/s	   48301 B/op	      64 allocs/op
BenchmarkBboltStore_PutSCCode/size=16384-24          	    7575	     79509 ns/op	 206.06 MB/s	   48282 B/op	      64 allocs/op
BenchmarkBboltStore_PutSCCode/size=16384-24          	    7762	     74936 ns/op	 218.64 MB/s	   48294 B/op	      64 allocs/op
BenchmarkBboltStore_PutSCCode/size=131072-24         	    2404	    294428 ns/op	 445.17 MB/s	  296565 B/op	     127 allocs/op
BenchmarkBboltStore_PutSCCode/size=131072-24         	    1557	    328247 ns/op	 399.31 MB/s	  296705 B/op	     127 allocs/op
BenchmarkBboltStore_PutSCCode/size=131072-24         	    2167	    256203 ns/op	 511.59 MB/s	  296571 B/op	     127 allocs/op
BenchmarkBboltStore_GetSCCode/size=256-24            	  377374	      1722 ns/op	 148.64 MB/s	    1144 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=256-24            	  329385	      2082 ns/op	 122.93 MB/s	    1144 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=256-24            	  431204	      1680 ns/op	 152.36 MB/s	    1144 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=2048-24           	  216541	      2635 ns/op	 777.20 MB/s	    2568 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=2048-24           	  210974	      2858 ns/op	 716.63 MB/s	    2568 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=2048-24           	  197630	      3255 ns/op	 629.26 MB/s	    2568 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=16384-24          	   50040	     14076 ns/op	1163.99 MB/s	   16904 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=16384-24          	   54357	     12273 ns/op	1334.92 MB/s	   16904 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=16384-24          	   45895	     13659 ns/op	1199.52 MB/s	   16904 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=131072-24         	   10602	     80114 ns/op	1636.07 MB/s	  131593 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=131072-24         	   12763	     42868 ns/op	3057.57 MB/s	  131593 B/op	      10 allocs/op
BenchmarkBboltStore_GetSCCode/size=131072-24         	   13219	     46423 ns/op	2823.44 MB/s	  131593 B/op	      10 allocs/op
BenchmarkWriteBatch_AddSCCode-24                     	 3250131	       181.0 ns/op	      24 B/op	       1 allocs/op
BenchmarkWriteBatch_AddSCCode-24                     	 3438570	       175.6 ns/op	      24 B/op	       1 allocs/op
BenchmarkWriteBatch_AddSCCode-24                     	 3440203	       159.3 ns/op	      24 B/op	       1 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=1-24               	   10000	     51802 ns/op	   12614 B/op	     106 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=1-24               	   10000	     52489 ns/op	   12616 B/op	     106 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=1-24               	   10000	     52368 ns/op	   12619 B/op	     106 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=16-24              	    2792	    242021 ns/op	  120829 B/op	     283 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=16-24              	    2617	    232552 ns/op	  120825 B/op	     283 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=16-24              	    2613	    259580 ns/op	  120849 B/op	     283 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=128-24             	     322	   1800361 ns/op	  956291 B/op	    1700 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=128-24             	     370	   1635501 ns/op	  955331 B/op	    1700 allocs/op
BenchmarkFlushBatch_WithSCCodes/n=128-24             	     394	   1687019 ns/op	  955012 B/op	    1700 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/storage	140.169s
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/structures
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkSCIDVariables_Marshal_Msgpack-24        	  488100	      1348 ns/op	    1033 B/op	       6 allocs/op
BenchmarkSCIDVariables_Marshal_Msgpack-24        	  488479	      1381 ns/op	    1033 B/op	       6 allocs/op
BenchmarkSCIDVariables_Marshal_Msgpack-24        	  493494	      1356 ns/op	    1033 B/op	       6 allocs/op
BenchmarkSCIDVariables_Marshal_Typed-24          	10030072	        67.03 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCIDVariables_Marshal_Typed-24          	 6280485	        91.71 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCIDVariables_Marshal_Typed-24          	 6443520	        96.96 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCIDVariables_Unmarshal_Msgpack-24      	  163374	      3959 ns/op	     856 B/op	      36 allocs/op
BenchmarkSCIDVariables_Unmarshal_Msgpack-24      	  142863	      4005 ns/op	     856 B/op	      36 allocs/op
BenchmarkSCIDVariables_Unmarshal_Msgpack-24      	  153213	      4011 ns/op	     856 B/op	      36 allocs/op
BenchmarkSCIDVariables_Unmarshal_Typed-24        	  465736	      1269 ns/op	     664 B/op	      30 allocs/op
BenchmarkSCIDVariables_Unmarshal_Typed-24        	  537022	      1263 ns/op	     664 B/op	      30 allocs/op
BenchmarkSCIDVariables_Unmarshal_Typed-24        	  434895	      1233 ns/op	     664 B/op	      30 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Msgpack-24        	 1759766	       372.1 ns/op	     112 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Msgpack-24        	 1657863	       361.0 ns/op	     112 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Msgpack-24        	 1724593	       358.9 ns/op	     112 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Typed-24          	1000000000	         0.4533 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Typed-24          	1000000000	         0.4644 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Marshal_Typed-24          	1000000000	         0.4343 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Marshal_TypedAppend-24    	422827318	         1.369 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Marshal_TypedAppend-24    	452284375	         1.455 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Marshal_TypedAppend-24    	427033801	         1.463 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Msgpack-24      	 1234189	       496.3 ns/op	      72 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Msgpack-24      	 1286622	       494.4 ns/op	      72 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Msgpack-24      	 1235952	       471.8 ns/op	      72 B/op	       2 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Typed-24        	537435518	         1.041 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Typed-24        	549333337	         1.101 ns/op	       0 B/op	       0 allocs/op
BenchmarkAddrSCIDEntry_Unmarshal_Typed-24        	561617908	         1.100 ns/op	       0 B/op	       0 allocs/op
BenchmarkInstallRecord_Marshal_Msgpack-24        	  845058	       629.6 ns/op	     280 B/op	       5 allocs/op
BenchmarkInstallRecord_Marshal_Msgpack-24        	 1000000	       612.3 ns/op	     280 B/op	       5 allocs/op
BenchmarkInstallRecord_Marshal_Msgpack-24        	 1000000	       661.5 ns/op	     280 B/op	       5 allocs/op
BenchmarkInstallRecord_Unmarshal_Msgpack-24      	 1000000	       621.5 ns/op	     192 B/op	       4 allocs/op
BenchmarkInstallRecord_Unmarshal_Msgpack-24      	 1000000	       604.6 ns/op	     192 B/op	       4 allocs/op
BenchmarkInstallRecord_Unmarshal_Msgpack-24      	 1000000	       615.8 ns/op	     192 B/op	       4 allocs/op
BenchmarkClassMeta_Marshal_Msgpack-24            	  425534	      1485 ns/op	     680 B/op	      11 allocs/op
BenchmarkClassMeta_Marshal_Msgpack-24            	  410918	      1430 ns/op	     680 B/op	      11 allocs/op
BenchmarkClassMeta_Marshal_Msgpack-24            	  464985	      1476 ns/op	     680 B/op	      11 allocs/op
BenchmarkClassMeta_Unmarshal_Msgpack-24          	  489408	      1312 ns/op	     312 B/op	       9 allocs/op
BenchmarkClassMeta_Unmarshal_Msgpack-24          	  384265	      1369 ns/op	     312 B/op	       9 allocs/op
BenchmarkClassMeta_Unmarshal_Msgpack-24          	  475744	      1281 ns/op	     312 B/op	       9 allocs/op
BenchmarkClassMeta_Marshal_Typed-24              	 2576197	       235.5 ns/op	     288 B/op	       2 allocs/op
BenchmarkClassMeta_Marshal_Typed-24              	 2487352	       236.7 ns/op	     288 B/op	       2 allocs/op
BenchmarkClassMeta_Marshal_Typed-24              	 2322169	       245.1 ns/op	     288 B/op	       2 allocs/op
BenchmarkClassMeta_Unmarshal_Typed-24            	 2093302	       289.8 ns/op	     120 B/op	       7 allocs/op
BenchmarkClassMeta_Unmarshal_Typed-24            	 2098201	       286.3 ns/op	     120 B/op	       7 allocs/op
BenchmarkClassMeta_Unmarshal_Typed-24            	 1987249	       282.1 ns/op	     120 B/op	       7 allocs/op
BenchmarkClassMeta_MarshalTypedAppend-24         	10420863	        55.53 ns/op	       0 B/op	       0 allocs/op
BenchmarkClassMeta_MarshalTypedAppend-24         	10682010	        47.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkClassMeta_MarshalTypedAppend-24         	12803742	        53.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkSCTXParse_Turbo_Marshal_Msgpack-24      	  449451	      1355 ns/op	    1009 B/op	       5 allocs/op
BenchmarkSCTXParse_Turbo_Marshal_Msgpack-24      	  412614	      1314 ns/op	    1009 B/op	       5 allocs/op
BenchmarkSCTXParse_Turbo_Marshal_Msgpack-24      	  537657	      1292 ns/op	    1009 B/op	       5 allocs/op
BenchmarkSCTXParse_Turbo_Unmarshal_Msgpack-24    	  400359	      1454 ns/op	     408 B/op	       6 allocs/op
BenchmarkSCTXParse_Turbo_Unmarshal_Msgpack-24    	  395754	      1502 ns/op	     408 B/op	       6 allocs/op
BenchmarkSCTXParse_Turbo_Unmarshal_Msgpack-24    	  389920	      1520 ns/op	     408 B/op	       6 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/structures	40.047s

--- TruncateToHeight sweep (July 2026, -benchtime=3x -count=6; see DOCS/BENCHMARKS.md "TruncateToHeight reorg rollback") ---
goos: windows
goarch: amd64
pkg: github.com/hypergnomon/hypergnomon/storage
cpu: 13th Gen Intel(R) Core(TM) i7-13700HX
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1706167 ns/op	        10.00 affectedSCs	 1029053 B/op	   13520 allocs/op
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1615500 ns/op	        10.00 affectedSCs	 1028557 B/op	   13517 allocs/op
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1677300 ns/op	        10.00 affectedSCs	 1028237 B/op	   13514 allocs/op
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1691400 ns/op	        10.00 affectedSCs	 1027992 B/op	   13510 allocs/op
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1627867 ns/op	        10.00 affectedSCs	 1028290 B/op	   13516 allocs/op
BenchmarkTruncateToHeight/scids=2000/addrs=512/depth=10/S=10-24         	       3	   1684933 ns/op	        10.00 affectedSCs	 1028504 B/op	   13515 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   4304933 ns/op	        10.00 affectedSCs	 2077618 B/op	   31574 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   4552200 ns/op	        10.00 affectedSCs	 2078528 B/op	   31570 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   5504067 ns/op	        10.00 affectedSCs	 2079189 B/op	   31573 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   7261633 ns/op	        10.00 affectedSCs	 2076552 B/op	   31568 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   8264233 ns/op	        10.00 affectedSCs	 2076344 B/op	   31570 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=10/S=10-24         	       3	   7400267 ns/op	        10.00 affectedSCs	 2075960 B/op	   31566 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  37298967 ns/op	        10.00 affectedSCs	 6744514 B/op	  104237 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  26440567 ns/op	        10.00 affectedSCs	 6738648 B/op	  104228 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  24542933 ns/op	        10.00 affectedSCs	 6741584 B/op	  104232 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  33309267 ns/op	        10.00 affectedSCs	 6743730 B/op	  104234 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  33578133 ns/op	        10.00 affectedSCs	 6743869 B/op	  104235 allocs/op
BenchmarkTruncateToHeight/scids=32000/addrs=512/depth=10/S=10-24        	       3	  26302567 ns/op	        10.00 affectedSCs	 6744642 B/op	  104238 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	  11667200 ns/op	        10.00 affectedSCs	 1676960 B/op	   25286 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	   9918933 ns/op	        10.00 affectedSCs	 1673570 B/op	   25276 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	   9832300 ns/op	        10.00 affectedSCs	 1673240 B/op	   25276 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	  10378567 ns/op	        10.00 affectedSCs	 1676410 B/op	   25282 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	   9905233 ns/op	        10.00 affectedSCs	 1674360 B/op	   25277 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=1/depth=10/S=10-24           	       3	  10118267 ns/op	        10.00 affectedSCs	 1673869 B/op	   25278 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  26291800 ns/op	        10.00 affectedSCs	 8723197 B/op	  121473 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  21150833 ns/op	        10.00 affectedSCs	 8678493 B/op	  121449 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  20910033 ns/op	        10.00 affectedSCs	 8678242 B/op	  121450 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  20778367 ns/op	        10.00 affectedSCs	 8680752 B/op	  121454 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  19097267 ns/op	        10.00 affectedSCs	 8680688 B/op	  121453 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=8192/depth=10/S=10-24        	       3	  20588633 ns/op	        10.00 affectedSCs	 8713408 B/op	  121470 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   6782700 ns/op	         1.000 affectedSCs	 2016965 B/op	   31044 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   7826800 ns/op	         1.000 affectedSCs	 2018920 B/op	   31045 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   9021367 ns/op	         1.000 affectedSCs	 2016240 B/op	   31040 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   7553600 ns/op	         1.000 affectedSCs	 2019106 B/op	   31046 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   6981833 ns/op	         1.000 affectedSCs	 2028912 B/op	   31043 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1-24         	       3	   7407467 ns/op	         1.000 affectedSCs	 2028549 B/op	   31041 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  10800567 ns/op	       100.0 affectedSCs	 3067184 B/op	   41075 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  10719267 ns/op	       100.0 affectedSCs	 3061880 B/op	   41037 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  11376167 ns/op	       100.0 affectedSCs	 3063701 B/op	   41019 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  10518667 ns/op	       100.0 affectedSCs	 3056658 B/op	   40958 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  12843267 ns/op	       100.0 affectedSCs	 3064962 B/op	   41008 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=100-24       	       3	  13625567 ns/op	       100.0 affectedSCs	 3070728 B/op	   41093 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  51799233 ns/op	      1000 affectedSCs	11026984 B/op	  123196 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  37421900 ns/op	      1000 affectedSCs	10977594 B/op	  122959 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  46675900 ns/op	      1000 affectedSCs	10986266 B/op	  123384 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  36945833 ns/op	      1000 affectedSCs	10912685 B/op	  122602 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  35146700 ns/op	      1000 affectedSCs	10978565 B/op	  122921 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=100/S=1000-24      	       3	  33452733 ns/op	      1000 affectedSCs	11038805 B/op	  123102 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   6872467 ns/op	         1.000 affectedSCs	 1986050 B/op	   30630 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   7419500 ns/op	         1.000 affectedSCs	 1985858 B/op	   30628 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   8395933 ns/op	         1.000 affectedSCs	 1985496 B/op	   30626 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   6401633 ns/op	         1.000 affectedSCs	 1985560 B/op	   30626 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   8027800 ns/op	         1.000 affectedSCs	 1985864 B/op	   30628 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1/S=1-24           	       3	   7095000 ns/op	         1.000 affectedSCs	 1988114 B/op	   30630 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   7649100 ns/op	         1.000 affectedSCs	 2300040 B/op	   34735 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   8901300 ns/op	         1.000 affectedSCs	 2300466 B/op	   34738 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   8762533 ns/op	         1.000 affectedSCs	 2303400 B/op	   34744 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   8422833 ns/op	         1.000 affectedSCs	 2300402 B/op	   34737 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   7843267 ns/op	         1.000 affectedSCs	 2300480 B/op	   34738 allocs/op
BenchmarkTruncateToHeight/scids=8000/addrs=512/depth=1000/S=1-24        	       3	   7490667 ns/op	         1.000 affectedSCs	 2300104 B/op	   34735 allocs/op
PASS
ok  	github.com/hypergnomon/hypergnomon/storage	86.212s
