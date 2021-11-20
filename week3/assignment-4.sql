select 
  TO_CHAR(sti.ts, 'yyyy-mm') as year_month, 
  usc.channel, 
  count(distinct usc.userid) as uniqueUsers,
  count(distinct case when str.amount > 0 then usc.userid end) as paidUsers,
  round(convert(float,paidUsers)/convert(float, NULLIF(uniqueUsers, 0)), 2) as conversionRate,
  sum(str.amount) as grossRevenue,
  sum(case when str.refunded is False then str.amount end) as netRevenue
from raw_data.user_session_channel as usc
left join raw_data.session_timestamp as sti on usc.sessionid = sti.sessionid
left join raw_data.session_transaction as str on usc.sessionid = str.sessionid
group by year_month, channel
order by year_month desc, channel, uniqueUsers desc;

-- RESULT
-- 42 rows affected.
-- year_month	channel	uniqueusers	paidusers	conversionrate	grossrevenue	netrevenue
-- 2019-11	Facebook	688	25	0.04	1678	1678
-- 2019-11	Google	    688	26	0.04	2286	2235
-- 2019-11	Instagram	669	25	0.04	2116	2116
-- 2019-11	Naver	    667	26	0.04	2234	1987
-- 2019-11	Organic	    677	34	0.05	2626	2255
-- 2019-11	Youtube	    677	45	0.07	3532	3331
-- 2019-10	Facebook	698	29	0.04	1650	1641
-- 2019-10	Google	    699	30	0.04	2150	2098
-- 2019-10	Instagram	707	33	0.05	2568	2395
-- 2019-10	Naver	    713	32	0.04	2695	2695
-- 2019-10	Organic	    709	31	0.04	2762	2608
-- 2019-10	Youtube	    705	34	0.05	2492	2319
-- 2019-09	Facebook	597	27	0.05	2270	2270
-- 2019-09	Google	    599	25	0.04	1872	1691
-- 2019-09	Instagram	588	20	0.03	1260	1122
-- 2019-09	Naver	    592	21	0.04	1996	1996
-- 2019-09	Organic	    592	22	0.04	1267	1267
-- 2019-09	Youtube	    588	15	0.03	1301	1301
-- 2019-08	Facebook	611	18	0.03	1009	1009
-- 2019-08	Google	    610	27	0.04	2210	1894
-- 2019-08	Instagram	621	28	0.05	2129	2001
-- 2019-08	Naver	    626	22	0.04	1829	1551
-- 2019-08	Organic	    608	26	0.04	1643	1606
-- 2019-08	Youtube	    614	18	0.03	987	950
-- 2019-07	Facebook	558	32	0.06	2222	2144
-- 2019-07	Google	    556	21	0.04	1558	1385
-- 2019-07	Instagram	567	24	0.04	1896	1766
-- 2019-07	Naver	    553	19	0.03	1547	1547
-- 2019-07	Organic	    557	22	0.04	1600	1600
-- 2019-07	Youtube	    564	36	0.06	2210	2037
-- 2019-06	Facebook	414	22	0.05	1578	1578
-- 2019-06	Google	    412	13	0.03	947	947
-- 2019-06	Instagram	410	21	0.05	1462	1418
-- 2019-06	Naver	    398	15	0.04	1090	1090
-- 2019-06	Organic	    416	14	0.03	1129	940
-- 2019-06	Youtube	    400	17	0.04	1042	1042
-- 2019-05	Facebook	247	14	0.06	1199	997
-- 2019-05	Google	    253	10	0.04	580	580
-- 2019-05	Instagram	234	11	0.05	959	770
-- 2019-05	Naver	    237	11	0.05	867	844
-- 2019-05	Organic	    238	17	0.07	1846	1571
-- 2019-05	Youtube	    244	9	0.04	529	529