WITH Block_Votes_Shosenkyo AS (
    SELECT
        b.block_id,
        c.party_id,
        SUM(vs.votes) AS total_votes
    FROM
        Votes_Shosenkyo vs
    JOIN Candidates c ON vs.candidate_id = c.candidate_id
    JOIN Municipalities m ON vs.municipality_id = m.municipality_id
    JOIN Districts d ON m.district_id = d.district_id
    JOIN Prefectures pf ON d.prefecture_id = pf.prefecture_id
    JOIN Blocks b ON pf.block_id = b.block_id
    GROUP BY
        b.block_id, c.party_id
),
-- Apply the D'Hondt Method for each block
D_Hondt_Calculation_Shosenkyo AS (
    SELECT
        bv.block_id,
        bv.party_id,
        (bv.total_votes / CAST(s.elect_rank AS FLOAT)) AS adjusted_votes,
        s.elect_rank
    FROM
        Block_Votes_Shosenkyo bv
    JOIN (
        SELECT ROW_NUMBER() OVER () AS elect_rank
        FROM Districts d
        JOIN Prefectures pf ON d.prefecture_id = pf.prefecture_id
        JOIN Blocks b ON pf.block_id = b.block_id
    ) s ON 1=1
),
Shosenkyo_Wins AS (
    SELECT
        block_id,
        party_id,
        COUNT(*) AS seats_won
    FROM (
        SELECT
            block_id,
            party_id,
            adjusted_votes,
            ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY adjusted_votes DESC) AS rank
        FROM
            D_Hondt_Calculation_Shosenkyo
    ) Ranked_Votes
    WHERE
        rank <= (
            SELECT COUNT(*) 
            FROM Districts d
            JOIN Prefectures pf ON d.prefecture_id = pf.prefecture_id
            JOIN Blocks b ON pf.block_id = b.block_id
            WHERE b.block_id = Ranked_Votes.block_id
        )
    GROUP BY
        block_id,
        party_id
    ORDER BY
        block_id, seats_won DESC
),
Block_Votes_Hireidaihyo AS (
    SELECT
        b.block_id,
        p.party_id,
        SUM(vh.votes) AS total_votes
    FROM
        Votes_Hireidaihyo vh
        JOIN Municipalities m ON vh.municipality_id = m.municipality_id
        JOIN Districts d ON m.district_id = d.district_id
        JOIN Prefectures pf ON d.prefecture_id = pf.prefecture_id
        JOIN Blocks b ON pf.block_id = b.block_id
        JOIN Parties p ON vh.party_id = p.party_id
    GROUP BY
        b.block_id,
        p.party_id
),
D_Hondt_Calculation_Hireidaihyo AS (
    SELECT
        bv.block_id,
        bv.party_id,
        (bv.total_votes / CAST(s.elect_rank AS FLOAT)) AS adjusted_votes,
        s.elect_rank
    FROM
        Block_Votes_Hireidaihyo bv
    JOIN (
        SELECT 1 AS elect_rank
        UNION ALL SELECT 2
        UNION ALL SELECT 3
        UNION ALL SELECT 4
        UNION ALL SELECT 5
        UNION ALL SELECT 6
        UNION ALL SELECT 7
        UNION ALL SELECT 8
        UNION ALL SELECT 9
        UNION ALL SELECT 10
        UNION ALL SELECT 11
        UNION ALL SELECT 12
        UNION ALL SELECT 13
        UNION ALL SELECT 14
        UNION ALL SELECT 15
        UNION ALL SELECT 16
        UNION ALL SELECT 17
        UNION ALL SELECT 18
        UNION ALL SELECT 19
        UNION ALL SELECT 20
        UNION ALL SELECT 21
        UNION ALL SELECT 22
        UNION ALL SELECT 23
        UNION ALL SELECT 24
        UNION ALL SELECT 25
        UNION ALL SELECT 26
        UNION ALL SELECT 27
        UNION ALL SELECT 28
    ) s ON 1=1
),
Hireidaihyo_Wins AS (
    SELECT
        block_id,
        party_id,
        COUNT(*) AS seats_won
    FROM (
        SELECT
            block_id,
            party_id,
            adjusted_votes,
            ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY adjusted_votes DESC) AS rank
        FROM
            D_Hondt_Calculation_Hireidaihyo
    ) Ranked_Votes
    WHERE
        rank <= (SELECT num_elect FROM Blocks WHERE block_id = Ranked_Votes.block_id)
    GROUP BY
        block_id,
        party_id
    ORDER BY
        block_id, seats_won DESC
)
SELECT
    p.name AS party_name,
    COALESCE(sw.seats_won, 0) AS shosenkyo_seats_won,
    COALESCE(hw.seats_won, 0) AS hireidaihyo_seats_won,
    COALESCE(sw.seats_won, 0) + COALESCE(hw.seats_won, 0) AS total_seats_won
FROM
    Parties p
    LEFT JOIN (
        SELECT
            party_id,
            SUM(seats_won) AS seats_won
        FROM
            Shosenkyo_Wins
        GROUP BY
            party_id
    ) sw ON p.party_id = sw.party_id
    LEFT JOIN (
        SELECT
            party_id,
            SUM(seats_won) AS seats_won
        FROM
            Hireidaihyo_Wins
        GROUP BY
            party_id
    ) hw ON p.party_id = hw.party_id
ORDER BY
    total_seats_won DESC;
