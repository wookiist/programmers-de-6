SELECT mu.month, COUNT(*) mau
FROM (
	SELECT DISTINCT TO_CHAR(st.ts, 'yyyy-mm') AS month, usc.userid
	FROM raw_data.user_session_channel AS usc
	JOIN raw_data.session_timestamp AS st ON usc.sessionid = st.sessionid
) mu
GROUP BY mu.month
ORDER BY mu.month;