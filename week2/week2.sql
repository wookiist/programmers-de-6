SELECT mu.month, COUNT(*) mau
FROM (
	SELECT DISTINCT TO_CHAR(st.ts, 'yyyy-mm') AS month, usc.userid
	FROM raw_data.user_session_channel AS usc
	JOIN raw_data.session_timestamp AS st ON usc.sessionid = st.sessionid
) mu
GROUP BY mu.month
ORDER BY mu.month;

-- Result
-- 2019-05	281
-- 2019-06	459
-- 2019-07	623
-- 2019-08	662
-- 2019-09	639
-- 2019-10	763
-- 2019-11	721