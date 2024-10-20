WITH District_Winners AS (
    SELECT d.district_id,
        c.candidate_id,
        c.party_id,
        SUM(vs.votes) AS total_votes
    FROM Votes_Shosenkyo vs
        JOIN Candidates c ON vs.candidate_id = c.candidate_id
        JOIN Districts d ON c.district_id = d.district_id
    GROUP BY d.district_id,
        c.candidate_id
),
Max_Votes_Per_District AS (
    SELECT district_id,
        MAX(total_votes) AS max_votes
    FROM District_Winners
    GROUP BY district_id
),
Shosenkyo_Wins AS (
    SELECT
        dw.district_id,
        dw.candidate_id,
        dw.party_id,
        dw.total_votes
    FROM
        District_Winners dw
        JOIN Max_Votes_Per_District mvpd
        ON dw.district_id = mvpd.district_id
        AND dw.total_votes = mvpd.max_votes
)
SELECT
    p.name AS party_name,
    IFNULL(CAST(ROUND(seats_won * (465.0 / 289)) AS INTEGER), 0) AS total_seats_won
FROM
    Parties p
    LEFT JOIN (
        SELECT
            party_id,
            COUNT(*) AS seats_won
        FROM
            Shosenkyo_Wins
        GROUP BY
            party_id
    ) sw ON p.party_id = sw.party_id
ORDER BY
    total_seats_won DESC;