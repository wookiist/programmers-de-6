-- week3, assignment-3

SELECT userid, sum(amount) amount
FROM raw_data.session_transaction AS st
LEFT JOIN raw_data.user_session_channel AS usc ON st.sessionid = usc.sessionid
GROUP BY 1
ORDER BY amount DESC
LIMIT 10;

-- RESULT
-- userid	amount
-- 989	    743
-- 772	    556
-- 1615	    506
-- 654	    488
-- 1651     463
-- 973	    438
-- 262	    422
-- 1099	    421
-- 2682	    414
-- 891	    412


-- 번외로, net revenue 계산은 다음처럼 구현했습니다
SELECT userid, sum(amount) amount
FROM raw_data.session_transaction AS st
LEFT JOIN raw_data.user_session_channel AS usc ON st.sessionid = usc.sessionid
WHERE st.refunded is False
GROUP BY 1
ORDER BY amount DESC
LIMIT 10;

-- RESULT
-- userid	amount
-- 989	    743
-- 772	    556
-- 1615	    506
-- 654	    488
-- 1651	    463
-- 973	    438
-- 262	    422
-- 2682 	414
-- 891	    412
-- 1085	    411