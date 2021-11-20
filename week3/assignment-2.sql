-- week3, assignment-2

WITH last_tb AS (
  SELECT tb.userid, tb.channel
  FROM (
    SELECT userid, ts, channel, ROW_NUMBER() OVER (partition by userid order by ts DESC) seq
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
    ORDER BY 1) tb
  WHERE 1=1
    and tb.seq = 1  
)
SELECT tb.userid, tb.channel first_channel, lt.channel last_channel
FROM (
  SELECT userid, ts, channel, ROW_NUMBER() OVER (partition by userid order by ts ASC) seq
  FROM raw_data.user_session_channel usc
  JOIN raw_data.session_timestamp st ON usc.sessionid = st.sessionid
  ORDER BY 1) tb
JOIN last_tb lt ON tb.userid = lt.userid
WHERE 1=1
  and tb.seq = 1;

-- RESULT
-- 2738	Youtube	Naver
-- 2744	Organic	Organic
-- 2746	Organic	Google
-- 2750	Instagram	Facebook
-- 2758	Google	Youtube
-- 2762	Youtube	Instagram
-- 2766	Naver	Facebook
-- 2768	Organic	Google
-- 2770	Youtube	Naver
-- 2784	Facebook	Naver
-- 2788	Youtube	Youtube
-- 2790	Organic	Youtube
-- 2794	Instagram	Instagram
-- 2798	Instagram	Instagram
-- 2806	Instagram	Naver
-- 3195	Google	Instagram
-- 2668	Organic	Facebook
-- 2670	Youtube	Instagram
-- 2676	Facebook	Youtube
-- 2678	Organic	Organic
-- 2680	Google	Instagram
-- 2682	Naver	Naver
-- 2692	Naver	Facebook
-- 2704	Facebook	Facebook
-- 2706	Youtube	Facebook
-- 2710	Naver	Google
-- 2712	Facebook	Instagram
-- 2714	Facebook	Google
-- 2716	Youtube	Instagram
-- 2720	Instagram	Google
-- 2722	Google	Youtube
-- 2724	Youtube	Youtube
-- 2748	Naver	Facebook
-- 2752	Google	Organic
-- 2754	Youtube	Instagram
-- 2760	Instagram	Organic
-- 2772	Youtube	Instagram
-- 2776	Instagram	Facebook
-- 2778	Google	Google
-- 2780	Google	Youtube
-- 2792	Naver	Google
-- 2802	Google	Naver
-- 2804	Organic	Instagram
-- 2808	Google	Naver
-- 3070	Youtube	Youtube
-- 5722	Youtube	Instagram
-- ...