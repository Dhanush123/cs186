DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v, inducted, caplayers, lslgdata, williemays, toppayzero, toppayone, toppay;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT max(era) 
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height), count(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height), count(*)
  FROM people
  GROUP BY birthyear
  HAVING avg(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, HallofFame.playerid, yearid
  FROM HallofFame 
  INNER JOIN people
  ON HallofFame.playerID = people.playerID
  WHERE inducted = 'Y'
  ORDER BY yearid DESC
;

CREATE VIEW inducted(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, hall.playerid, yearid
    FROM HallofFame AS hall
    INNER JOIN people
    ON hall.playerid = people.playerid
    WHERE inducted = 'Y'
    ORDER BY yearid DESC
;

CREATE VIEW caplayers(schoolid,playerid)
AS
  SELECT collegeplaying.schoolid, collegeplaying.playerid 
    FROM schools
    INNER JOIN collegeplaying
    ON schools.schoolid = collegeplaying.schoolid
    WHERE schoolState = 'CA'
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, inducted.playerid, schoolid, yearid
  FROM inducted
  INNER JOIN caplayers
  ON inducted.playerid = caplayers.playerid
  ORDER BY yearid DESC, schoolid, playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT inducted.playerid, namefirst, namelast, schoolid
  FROM inducted
  LEFT OUTER JOIN collegeplaying
  ON inducted.playerid = collegeplaying.playerid
  ORDER BY playerid DESC, schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT batting.playerid,namefirst,namelast,yearid,(((H-H2B-H3B-HR)+(2*H2B)+(3*H3B)+(4*HR))::float)/AB::float as slg
  FROM batting
  INNER JOIN people
  ON batting.playerid = people.playerid
  WHERE AB > 50
  ORDER BY slg DESC, yearid, playerid
  LIMIT 10
;

CREATE VIEW lslgdata(playerid, namefirst, namelast, lslg)
AS
  SELECT people.playerid,namefirst,namelast,(sum((H-H2B-H3B-HR)+(2*H2B)+(3*H3B)+(4*HR))::float)/sum(AB)::float as lslg
  FROM batting
  INNER JOIN people
  ON batting.playerid = people.playerid
  GROUP BY people.playerid
  HAVING sum(AB) > 50
  ORDER BY lslg DESC, playerid
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT *
  FROM lslgdata
  LIMIT 10
;


CREATE VIEW williemays(playerid,namefirst,namelast,lslg)
AS
  SELECT people.playerid,namefirst,namelast,(sum((H-H2B-H3B-HR)+(2*H2B)+(3*H3B)+(4*HR))::float)/sum(AB)::float as lslg
  FROM batting
  INNER JOIN people
  ON batting.playerid = people.playerid
  WHERE people.playerid='mayswi01'
  GROUP BY people.playerid
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT lslgdata.namefirst, lslgdata.namelast, lslgdata.lslg
  FROM lslgdata, williemays
  WHERE lslgdata.lslg > williemays.lslg
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid,min(salary),max(salary),avg(salary),stddev(salary)
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT 1, 1, 1, 1 -- replace this line
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT cur.yearid,cur.min-prev.min,cur.max-prev.max,cur.avg-prev.avg
  FROM q4i as cur
  INNER JOIN q4i as prev
  ON prev.yearid = cur.yearid-1
  ORDER BY yearid
;

CREATE VIEW toppayzero(playerid, salary, yearid)
AS
  SELECT playerid, salary, yearid
  FROM salaries AS s1
  WHERE s1.salary >= ALL (SELECT s2.salary FROM salaries AS s2 WHERE s2.yearid = 2000)
  AND s1.yearid = 2000
;

CREATE VIEW toppayone(playerid, salary, yearid)
AS
  SELECT playerid, salary, yearid
  FROM salaries AS s1
  WHERE s1.salary >= ALL (SELECT s2.salary FROM salaries AS s2 WHERE s2.yearid = 2001)
  AND s1.yearid = 2001
;

CREATE VIEW toppay(playerid, salary, yearid)
AS
  SELECT * FROM toppayzero
  UNION
  SELECT * FROM toppayone
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT toppay.playerid, namefirst, namelast, salary, yearid
  FROM toppay
  INNER JOIN people
  ON toppay.playerid = people.playerid
;

-- Question 4v
CREATE VIEW q4v(team, diffAvg)
AS
  SELECT ASF.teamid, max(salary)-min(salary)
  FROM allstarfull as ASF
  INNER JOIN salaries
  ON ASF.playerid = salaries.playerid and ASF.yearid = salaries.yearid
  WHERE ASF.yearid = 2016
  GROUP BY ASF.teamid
  ORDER BY teamid
;

