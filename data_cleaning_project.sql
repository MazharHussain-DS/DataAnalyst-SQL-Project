-- Data cleaning project

/* STEPS
1. Remove duplicates 
2. standardize the data
3. Handling null values or blank values
4. Remove any column/rows if required */


SELECT *
FROM layoffs;

-- creating similar table to work on it
CREATE TABLE layoffs_staging
like layoffs;

SELECT *
FROM layoffs_staging;

-- inserting data from layoffs table
insert layoffs_staging
select *
from layoffs;

-- first lets find out the duplicates but here we don't have any unique id so we will add a row_rank first.
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS ROW_NUM
FROM layoffs_staging;

-- CREATING A CTE
WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS ROW_NUM
FROM layoffs_staging
)
-- checking duplicates
SELECT *
FROM duplicate_cte
WHERE ROW_NUM > 1;
-- veryfying it
SELECT *
FROM layoffs_staging
WHERE company = 'yahoo';

-- create another table and copy all the data from layoffs_staging than we will put condition on row_num where it is greater than 1 we will delete it.alter
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `ROW_NUM` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 



SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS ROW_NUM
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE ROW_NUM > 1;

-- DELETING THE DUPLICATES
DELETE
FROM layoffs_staging2
WHERE ROW_NUM > 1;

SELECT *
FROM layoffs_staging2;

-- NOW LETS STANDARDIZE THE DATA
SELECT company,trim(company),location,trim(location)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company),
location = trim(location);

SELECT DISTINCT industry
from layoffs_staging2
ORDER BY 1; -- 1 means 1st column

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

SELECT DISTINCT country
from layoffs_staging2
order by 1;

/* SELECT *
FROM layoffs_staging2
WHERE country = 'united States.';

UPDATE layoffs_staging2
SET country = 'United States'
where country  = 'united States.' */

-- another method of fixing it
SELECT DISTINCT country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- change the data type to date from text.
SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging2;

-- CHANGING DATA TYPES
ALTER table layoffs_staging2
modify column `date` DATE;

-- Step 3: Handling a null values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null; -- finding those rows where percentage laid off and total laid off is null.

-- deleting it
DELETE
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null;

SELECT *
FROM layoffs_staging2
WHERE industry is null -- finding null and blank values in industry column.
OR industry = '';

SELECT *
FROM layoffs_staging2
where company ='Airbnb'; 

UPDATE layoffs_staging2
SET industry = null
where industry = '';

SELECT T1.industry,T2.industry
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
WHERE T1.industry is null
AND T2.industry is not null;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry is null
AND T2.industry is not null; -- populated the industry null values with themselves.


-- Droping the row_num column 
ALTER table layoffs_staging2
DROP COLUMN ROW_NUM;


SELECT *
FROM layoffs_staging2;

