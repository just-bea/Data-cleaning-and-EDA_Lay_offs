SELECT*
FROM layoffs

CREATE TABLE LAYOFFS_STAGING
LIKE LAYOFFS

-- SAVING RAW DATA
INSERT LAYOFFS_STAGING
SELECT *
FROM LAYOFFS

SELECT *
FROM LAYOFFS_STAGING;

-- REMOVING DUPLICATES
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY 
                          company, 
                          location,
                          industry,
                          total_laid_off, 
                          percentage_laid_off,
                          `date`,
                          stage,
                          country,
                          funds_raised_millions
                         ) as rownum
FROM LAYOFFS_STAGING;



WITH duplicate_CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY 
                              company, 
                              location,
                              industry,
                              total_laid_off, 
                              percentage_laid_off,
                              `date`,
                              stage,
                              country,
                              funds_raised_millions
                             ) as rownum
    FROM LAYOFFS_STAGING
)


SELECT * FROM duplicate_CTE
WHERE rownum > 1;

SELECT *
FROM LAYOFFS_STAGING
WHERE company = 'Casper'


CREATE TABLE Layoff_Staging2 (
    company text,
    location text,
    industry text, 
    total_laid_off TEXT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage text,
    country text,
    funds_raised_millions TEXT,
    rownum int
);

SELECT *
FROM layoff_staging2;
WHERE ROWNUM =2

INSERT INTO layoff_staging2
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY 
                          company, 
                          location,
                          industry,
                          total_laid_off, 
                          percentage_laid_off,
                          `date`,
                          stage,
                          country,
                          funds_raised_millions
                         ) as rownum
FROM LAYOFFS_STAGING;


SELECT *
FROM layoff_staging2
WHERE rownum > 1

DELETE 
FROM layoff_staging2
WHERE ROWNUM >1

-- STANDARDIZING DATA

SELECT company, TRIM(company)
FROM layoff_staging2

UPDATE layoff_staging2
SET company = TRIM(company)

SELECT industry 
FROM layoff_staging2
WHERE industry like 'Crypto%'
Group by industry

UPDATE layoff_staging2
SET industry ='Crypto'
WHERE industry like 'Crypto%'

SELECT DISTINCT location
FROM layoff_staging2

SELECT country
FROM layoff_staging2
WHERE country like 'United States%'
GROUP BY country

SELECT  trim( TRAILING '.' FROM Country)
FROM layoff_staging2
WHERE country LIKE 'United States%'

UPDATE layoff_staging2
SET country= trim( TRAILING '.' FROM Country)
WHERE country LIKE 'United States%'

SELECT `date`, 
STR_TO_DATE(`date`, '%m/%d/%Y') as format
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')  

SELECT `date` 
FROM layoff_staging2;

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` date 

-- REMOVING NULLS AND BLANKS


SELECT *
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL

DELETE
FROM layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL

SELECT *
FROM layoff_staging2
WHERE INDUSTRY = ' ' 

SELECT *
FROM layoff_staging2
WHERE company = 'Airbnb'


-- POPULATING THE BLANK SPACES IN THE INDUSTRY COLUMN


UPDATE layoff_staging2
SET industry = NULL 
WHERE industry = ' ' 

SELECT t1.industry, t2.industry
FROM layoff_staging2 T1
JOIN layoff_staging2 T2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL


UPDATE layoff_staging2 
set industry = 'Travel' 
where company = 'Airbnb'

UPDATE layoff_staging2 
set industry = 'Transportation' 
where company = 'Carvana'

UPDATE layoff_staging2 
set industry = 'Consumer' 
where company = 'Juul'

SELECT *
FROM layoff_staging2
WHERE company = 'Carvana'
 
 ALTER TABLE layoff_staging2
 DROP COLUMN ROWNUM


-- EXPLORATORY DATA ANALYSIS


SELECT *
FROM layoff_staging2


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_staging2;


-- COMPANIES THAT WENT OUT OF BUSINESS
SELECT *
FROM layoff_staging2
WHERE percentage_laid_off = 1
order by total_laid_off DESC


-- TOTAL OF PEOPLE LAID OFF BY COMPANY
SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC

-- FIRST AND LAST DATE CONTAINED IN THIS DATASET
SELECT MIN(`date`), MAX(`date`)   
FROM layoff_staging2 


-- TOTAL OF PEOPLE LAID OFF BY INDUSTRY
SELECT industry, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY industry
ORDER BY 2 DESC


-- TOTAL OF PEOPLE LAID OFF BY COUNTRY
SELECT country, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY country
ORDER BY 2 DESC

-- TOTAL OF PEOPLE LAID OFF PER DATE 
SELECT `date`, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY `date`
ORDER BY 2 DESC

-- TOTAL OF PEOPLE LAID OFF PER YEAR
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC

-- TOTAL OF PEOPLE LAID OFF PER THE STAGE OF THE COMPANY  
SELECT STAGE, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY STAGE
ORDER BY 2 DESC
 
-- TOTAL OF PEOPLE LAID OFF PER MONTH 
SELECT SUBSTRING(`DATE`, 1, 7) AS MONTH, SUM(total_laid_off)
FROM layoff_staging2 
WHERE SUBSTRING(`DATE`, 1, 7) is not null
GROUP BY MONTH
ORDER BY 1 

-- LOOKING AT THE ROLLING TOTAL PER MONTH AS WELL AS THE TOTAL PER MONTH INDIVIDUALLY 
WITH rollingcte 
as (
SELECT SUBSTRING(`DATE`, 1, 7) AS MONTH, SUM(total_laid_off) AS TOTAL_OFF
FROM layoff_staging2 
WHERE SUBSTRING(`DATE`, 1, 7) is not null
GROUP BY MONTH
ORDER BY 1 
)
SELECT `MONTH`, SUM(TOTAL_OFF) OVER (ORDER BY `MONTH`) AS ROLLING_TOTAL, TOTAL_OFF
FROM rollingcte

-- RANKING COMPANIES BASED ON HOW MANY PEOPLE WERE LET GO IN EACH YEAR. 2 CTEs WHERE CREATED IN ORDER TO LOOK AT THE TOP 5 
WITH rankingcte AS
(
SELECT company, YEAR(`date`) AS years , SUM(total_laid_off) as total_off
FROM layoff_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc 
), companyyearrank as
(SELECT *, DENSE_RANK() OVER( PARTITION BY  years ORDER BY total_off DESC) as ranking
FROM rankingcte
WHERE years IS NOT NULL
ORDER BY ranking)

SELECT* 
FROM COMPANYYEARRANK
WHERE RANKING <= 5