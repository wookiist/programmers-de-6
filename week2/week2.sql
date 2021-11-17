SELECT TO_CHAR(st.ts, 'yyyy-mm') AS month, COUNT(DISTINCT usc.userid) mau
FROM raw_data.user_session_channel AS usc
JOIN raw_data.session_timestamp AS st ON usc.sessionid = st.sessionid
GROUP BY month
ORDER BY month;

-- Result
-- 2019-05	281
-- 2019-06	459
-- 2019-07	623
-- 2019-08	662
-- 2019-09	639
-- 2019-10	763
-- 2019-11	721
