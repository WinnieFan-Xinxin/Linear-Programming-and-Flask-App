# Linear_Programming_Model__FlaskWebApp
This project includes two parts:   
(1) Linear Optimization Models based in Python for the natural gas market   
to predict the equilibrium demand price and volume of gas transit  
with the objective function of minimizing costs of supply, transit, export and storage,  
and contraints of supply-demand balances, and capacity restrictions.   
(2) Web Application --  
(web application exchanges data with MySql database, previously it linked to MS SQL Server database)  
the 'application.py' file is the script for flask web application which has multiple webpages to accomplish the following main functions:   
(2.1) Create a new case recorded by database  
(2.2) Upload excel data files for a certain case to database   
(2.3) Select and Run a linear optimization model with customized parameters, and export model outputs to database  
