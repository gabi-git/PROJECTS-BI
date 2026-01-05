SELECT * 
FROM world_layoffs.layoffs;

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *, 
ROW_NUMBER() OVER( PARTITION BY
company , location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

WITH duplicate_cte AS (
SELECT *, 
ROW_NUMBER() OVER( PARTITION BY
company , location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num >1;

SELECT *
from layoffs_staging
where company = 'Casper';
    
    
    -- deleting  duplicated rows 
WITH duplicate_cte AS (
SELECT *, 
ROW_NUMBER() OVER( 
PARTITION BY
company , location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
DELETE						 -- the only change and this way is not working!!!!! error cofe 1288 duplicte_cte is not updateble
FROM duplicate_cte 
WHERE row_num >1;


   
  
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
from layoffs_staging2;

INSERT INTO layoffs_staging2    -- INSERT INTOOOOOOOOO
SELECT *, 
ROW_NUMBER() OVER( 
PARTITION BY
company , location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;
    
DELETE
from layoffs_staging2
WHERE row_num > 1;    
    
Select *
from layoffs_staging2
;    
    
    
 -- --------------------- standardising and fixingggggg
 
 SELECT DISTINCT(company)
 FROM layoffs_staging2;
    
  SELECT  company, TRIM(company)
 FROM layoffs_staging2;
       
    -- ------------------------------- UPDATING THE COLUMN WITH NEW COLUMN DATA
UPDATE layoffs_staging2
SET company = TRIM(company);

 SELECT  DISTINCT industry
 FROM layoffs_staging2
 ORDER BY 1;
    
 SELECT  *
 FROM layoffs_staging2
 WHERE industry LIKE 'Crypto%';
 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%'; 
    
SELECT  DISTINCT country
 FROM layoffs_staging2
 ORDER BY 1;  
    
-- ----------- REMOVING COMA from the end of United Sthates.


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country like 'United States%'
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT  *
 FROM layoffs_staging2;
    
-- --------------------------------------------- changing column data type!!

SELECT `date`, STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;
    
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');
    
SELECT `date`
FROM layoffs_staging2;  

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;  

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Bally%';

-- ------------------------------------------- fill in /update row cell with sth
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2  -- ----------- JOINING on itself to fill in the blank space :o
	on t1.company = t2.company 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

SELECT *
FROM layoffs_staging2

ALTER TABLE layoffs_staging2    -- getting rid off COLUMN row_num
DROP COLUMN row_num;


-- what I did in here:
-- removed duplicate, 
-- standardised the data
-- dealt with null/blank values
-- removed useless columns/rows




