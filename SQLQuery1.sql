create database COVID
use COVID

--Step 1: Create the table
IF OBJECT_ID('CovidDeaths') is not null DROP TABLE CovidDeaths

CREATE TABLE CovidDeaths(
iso_code varchar(200),
continent varchar(200),
[location] varchar(200),
[date] date,
[population] bigint,
total_cases int,
new_cases int,
new_cases_smoothed float,
total_deaths int,
new_deaths int,
new_deaths_smoothed float,
total_cases_per_million float,
new_cases_per_million float,
new_cases_smoothed_per_million float,
total_deaths_per_million float,
new_deaths_per_million float,
new_deaths_smoothed_per_million float,
reproduction_rate float,
icu_patients int,
icu_patients_per_million float,
hosp_patients int,
hosp_patients_per_million float,
weekly_icu_admissions int,
weekly_icu_admissions_per_million float,
weekly_hosp_admissions int,
weekly_hosp_admissions_per_million float
)

select * from CovidDeaths

--Step 2: Import data from excel
BULK INSERT CovidDeaths
FROM 'D:\CAREER JOURNEY\Portfolio\COVID-19 Data Analysis Dashboard\CovidDeaths_Processed.csv'
WITH (
--FIELDTERMINATOR = ',',
--ROWTERMINATOR = '0x0A'
FORMAT='CSV')


IF OBJECT_ID('CovidVaccinations') is not null DROP TABLE CovidVaccinations

CREATE TABLE CovidVaccinations(
iso_code varchar(200),
continent varchar(200),
[location] varchar(200),
[date] date,
total_tests int,
new_tests int,
total_tests_per_thousand float,
new_tests_per_thousand float,
new_tests_smoothed float,
new_tests_smoothed_per_thousand float,
positive_rate float,
tests_per_case float,
tests_units varchar(200),
total_vaccinations bigint,
people_vaccinated bigint,
people_fully_vaccinated bigint,
total_boosters bigint,
new_vaccinations int,
new_vaccinations_smoothed float,
total_vaccinations_per_hundred float,
people_vaccinated_per_hundred float,
people_fully_vaccinated_per_hundred float,
total_boosters_per_hundred float,
new_vaccinations_smoothed_per_million float,
new_people_vaccinated_smoothed float,
population_density float,
median_age float,
aged_65_older float,
hospital_beds_per_thousand float,
life_expectancy float
)

BULK INSERT CovidVaccinations
FROM 'D:\CAREER JOURNEY\Portfolio\COVID-19 Data Analysis Dashboard\CovidVaccinations_Processed.csv'
WITH (
--FIELDTERMINATOR = ',',
--ROWTERMINATOR = '0x0A'
FORMAT='CSV')


--ANALYSIS BEGINS

--select * from CovidDeaths
--order by 3,4

--Select data that we are going to be using
Select location, date, total_cases, new_cases, total_deaths, population
from CovidDeaths
order by 1,2

--Looking at total cases VS total deaths
--Shows Likelihood of death if contract Covid
select Location, date, total_cases, total_deaths, DeathPercentage = round((total_deaths/cast(total_cases as float))*100, 2)
from CovidDeaths
--where location like '%states%'
order by 1,2

--Shows the percentage of infected population
select Location, date, population, total_cases, PatientPercentage = (total_cases/cast(population as float))*100
from CovidDeaths
order by 1,2

--Looking at countries with highest infection rate compared to population
select Location, population, max(total_cases) as HighestInfectionCount, max((total_cases/cast(population as float)))*100 as PercentPopulationInfected
from CovidDeaths
group by Location, population
order by PercentPopulationInfected desc

--Showing Countries with highest death count per population
Select Location, max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
where continent is not null
group by location
order by TotalDeathCount desc

--Showing highest death count by continent
Select continent, max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
where continent is not null
group by continent
order by TotalDeathCount desc

--Global Numbers
Select date, sum(new_cases) as GlobalCases, sum(new_deaths) as GlobalDeaths, cast(sum(new_deaths) as float)/NULLIF(sum(new_cases),0)*100 as GlobalDeathPercentage
from CovidDeaths
where continent is not null
group by date
order by 1,2

--Joining Tables and Calculating Values
Select cod.continent, cod.location, cod.date, cod.population, cov.new_vaccinations,
 sum(convert(bigint, new_vaccinations)) over (partition by cod.location order by cod.location, cod.date)
 as RollingPeopleVaccinated
from  CovidDeaths cod
join CovidVaccinations cov
 on cod.location = cov.location and cod.date = cov.date

 --	Using CTE
 with  PopVsVacc (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
 as
 (
 Select cod.continent, cod.location, cod.date, cod.population, cov.new_vaccinations,
 sum(convert(bigint, new_vaccinations)) over (partition by cod.location order by cod.location, cod.date)
 as RollingPeopleVaccinated
from  CovidDeaths cod
join CovidVaccinations cov
 on cod.location = cov.location and cod.date = cov.date
 where cod.continent is not null
 )
 select *, VaccinatedPercentage = (RollingPeopleVaccinated/cast(population as float))*100
 from PopVsVacc


 --VIEWS
Create view PatientDeathLikelihood as
select Location, date, total_cases, total_deaths, DeathPercentage = round((total_deaths/cast(total_cases as float))*100, 2)
from CovidDeaths

Create view InfectedPopulationPercent as
select Location, date, population, total_cases, PatientPercentage = (total_cases/cast(population as float))*100
from CovidDeaths

Create view HighestInfectionCountry as
select Location, population, max(total_cases) as HighestInfectionCount, max((total_cases/cast(population as float)))*100 as PercentPopulationInfected
from CovidDeaths
group by Location, population

Create view TotalDeathsByCountry as
Select Location, max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
where continent is not null
group by location

create view HighestDeathByContinent as
Select continent, max(cast(total_deaths as int)) as TotalDeathCount
from CovidDeaths
where continent is not null
group by continent

Create view GlobalDeathPercentage as
Select date, sum(new_cases) as GlobalCases, sum(new_deaths) as GlobalDeaths, cast(sum(new_deaths) as float)/NULLIF(sum(new_cases),0)*100 as GlobalDeathPercentage
from CovidDeaths
where continent is not null
group by date

create view PopulationVaccinatedPercentage as
 --	Using CTE
 with  PopVsVacc (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
 as
 (
 Select cod.continent, cod.location, cod.date, cod.population, cov.new_vaccinations,
 sum(convert(bigint, new_vaccinations)) over (partition by cod.location order by cod.location, cod.date)
 as RollingPeopleVaccinated
from  CovidDeaths cod
join CovidVaccinations cov
 on cod.location = cov.location and cod.date = cov.date
 where cod.continent is not null
 )
 select *, VaccinatedPercentage = (RollingPeopleVaccinated/cast(population as float))*100
 from PopVsVacc

