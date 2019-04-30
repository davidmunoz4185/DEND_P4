# DEND_P4

### SUMMARY ...

In this section we add description about the different files used:

* P4.ipynb: Jupyter Notebook with exactly the same content than etl.py in order to check performance in each task
* etl.py: Main python program with all the logic to load parquet files

### DIAGRAM ...

Now we can see how the "directories" are structured:

* BIN Directory:

```
.
├── aws
│   └── credentials.cfg
├── etl.py
├── P4.ipynb
└── README.md

```

* SONGs Directory:

```
.
├── _SUCCESS
├── year=0
│   ├── artist_id=AR051KA1187B98B2FF
│   │   └── part-00158-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1961
│   └── artist_id=ARH4Z031187B9A71F2
│       └── part-00165-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1964
│   └── artist_id=ARAJPHH1187FB5566A
│       └── part-00140-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1969
│   └── artist_id=ARMJAGH1187FB546F3
│       └── part-00082-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1972
│   └── artist_id=ARB29H41187B98F0EF
│       └── part-00001-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1982
│   └── artist_id=AR7G5I41187FB4CE6C
│       └── part-00174-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1984
│   └── artist_id=AR8ZCNI1187B9A069B
│       └── part-00139-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1985
│   └── artist_id=AR47JEX1187B995D81
│       └── part-00033-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1986
│   └── artist_id=ARIK43K1187B9AE54C
│       └── part-00153-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1987
│   └── artist_id=ARD842G1187B997376
│       └── part-00078-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1992
│   └── artist_id=AR0RCMP1187FB3F427
│       └── part-00107-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1993
│   └── artist_id=AR7ZKHQ1187B98DD73
│       └── part-00010-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1994
│   ├── artist_id=ARBEBBY1187B9B43DB
│   │   └── part-00113-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=ARNF6401187FB57032
│       └── part-00173-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1997
│   ├── artist_id=ARGUVEV1187B98BA17
│   │   └── part-00007-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=AROUOZZ1187B9ABE51
│       └── part-00127-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=1999
│   └── artist_id=AR3JMC51187B9AE49D
│       └── part-00071-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=2000
│   ├── artist_id=ARPBNLO1187FB3D52F
│   │   └── part-00108-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=ARWB3G61187FB49404
│       └── part-00109-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=2003
│   ├── artist_id=AR0IAWL1187B9A96D0
│   │   └── part-00130-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=AR558FS1187FB45658
│       └── part-00078-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=2004
│   ├── artist_id=ARMAC4T1187FB3FA4C
│   │   └── part-00151-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   ├── artist_id=ARP6N5A1187B99D1A3
│   │   └── part-00170-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   ├── artist_id=ARVBRGZ1187FB4675A
│   │   └── part-00125-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=ARYKCQI1187FB3B18F
│       └── part-00198-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=2005
│   ├── artist_id=AR62SOJ1187FB47BB5
│   │   └── part-00091-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
│   └── artist_id=ARDNS031187B9924F0
│       └── part-00042-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
├── year=2007
│   └── artist_id=ARXR32B1187FB57099
│       └── part-00079-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet
└── year=2008
    └── artist_id=AR8IEZO1187B99055E
        └── part-00117-36c4cadd-4c45-42b4-b279-c3a90ab0f83d.c000.snappy.parquet

```

* ARTISTs Directory:

```

├── part-00000-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00007-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00008-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00014-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00017-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00026-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00027-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00030-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00033-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00035-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00185-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00192-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00196-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00197-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00198-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
├── part-00199-0628f60f-30f2-4235-97bf-73a7537adcea-c000.snappy.parquet
└── _SUCCESS

``` 

* USERs Directory:

```
├── part-00000-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00002-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00006-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00008-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00009-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00010-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00012-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00014-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00018-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00024-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00028-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00029-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00036-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00187-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00188-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00193-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet
├── part-00194-1125cdd1-2590-4449-987c-b491342938e3-c000.snappy.parquet

```

* TIMETABLE DIR:

```
.
├── _SUCCESS
└── year=2018
    └── month=11
        ├── part-00000-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00001-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00002-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00003-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00004-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00005-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00006-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00007-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00008-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00009-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00010-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00011-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet
        ├── part-00012-8ca96031-5096-485b-bd57-163ac7eac96b.c000.snappy.parquet

```

* SONG_PLAYS DIR:

```
.
├── _SUCCESS
└── year=2018
    └── month=11
        └── part-00000-06bf5302-0528-4f61-b9f4-a8707cfd7615.c000.snappy.parquet
```

### HOW TO ...

First of all you must configure aws./credentials.cfg:

```

[AWS]
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''

[S3]
INPUT=..
OUTPUT=..

```

And then execute ...

```
python etl.py
```
