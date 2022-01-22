DO $$
--SELECT site_key, site_id, site_name FROM _common.dim_site WHERE site_name LIKE '%Enfield%'
DECLARE SiteKey text;
DECLARE StartDate timestamp without time zone;
DECLARE EndDate timestamp without time zone;
DECLARE SiteUTC interval;
BEGIN
	--*Params
	SiteKey:='615305f81fcafe000d34d3fb';
	SiteUTC:= interval '5 hours';
	StartDate:='2021-10-01'::timestamp without time zone;
	EndDate:='2021-11-30 23:59:59'::timestamp without time zone;
--debug
--EndDate:='2021-11-10'::timestamp without time zone;
	
	--SessionsPeriod:= '[2021-10-31, 2021-12-01]'::daterange;
	--RoundsPeriod:= '[2021-11-01, 2021-12-01)'::daterange;
	--*
	CREATE TABLE IF NOT EXISTS tbGGRRetVal
	(
		session_id text null,
		game_id text null,
		player_id text null,
		currency text null,
		session_created_at timestamp null,
		bonus boolean null,
		rounds integer null,
		credits numeric null,
		debits numeric null,
		bets numeric null,
		wins numeric null,
		rounds_date_hour timestamp null,
		session_setup smallint null,
		round_date date null
	);
	TRUNCATE TABLE tbGGRRetVal;	
	
	WITH _Sessions AS (
		SELECT
			encode(s.session_id, 'hex') AS session_id,
			encode(s.site_id, 'hex') AS customer_id,
			encode(s.game_id, 'hex') AS game_id,
			player_id, currency, ip, user_agent, created_at, cash, demo, locale, variant, start_balance, setup AS session_setup, date AS session_date
		FROM _transform.session AS s
		WHERE
			encode(s.site_id, 'hex') = SiteKey
			AND (s.date>=StartDate-interval '1 day' AND s.date<=EndDate+interval '1 day')
			AND cash = True
			AND demo = False
	)
	, _Rounds AS (
		SELECT
			encode(r.session_id, 'hex') AS session_id,
			s.game_id, 
			s.player_id, 
			s.currency, 
			s.created_at AS session_created_at,
			bonus, rounds, credits, debits, bets, wins, rounds_date_hour, setup AS round_setup, date AS round_date
		FROM _transform.session_activity_rounds_date_hour AS r
		INNER JOIN _Sessions AS s ON 
			encode(r.session_id, 'hex') = s.session_id
		WHERE 
			r.rounds_date_hour+SiteUTC>=StartDate 
			AND r.rounds_date_hour+SiteUTC<=EndDate
	)
	--SELECT COUNT(*) INTO nRetVal FROM _Rounds;--6585, 6:50 per day
	--RAISE NOTICE 'Count is %', nRetVal;

	INSERT INTO tbGGRRetVal
	SELECT * FROM _Rounds;
END$$;


SELECT 
	dg.game AS Game,
	raw.currency AS Currency,
	raw.round_date AS RoundDate,
	SUM(bets) AS Bets,
	SUM(wins) AS Wins,
	SUM(bets)-SUM(wins) AS GGR,
	ROUND(SUM(bets)*MAX(dc.rate_inverse), 2) AS Bets_EUR,
	ROUND(SUM(wins)*MAX(dc.rate_inverse), 2) AS Wins_EUR,
	ROUND(SUM(bets)*MAX(dc.rate_inverse)-SUM(wins)*MAX(dc.rate_inverse), 2) AS GGR_EUR
FROM tbGGRRetVal raw
INNER JOIN _common.dim_game dg ON
	raw.game_id = dg.game_id
LEFT JOIN _common.dim_currency dc ON
	raw.currency= dc.currency_code
	AND dc.base_code = 'EUR'
	AND raw.round_date BETWEEN dc.date_from AND dc.date_to 
GROUP BY
	dg.game,
	raw.currency,
	raw.round_date
ORDER BY 
	raw.round_date,
	dg.game
