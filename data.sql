CREATE DATABASE SampleDB;

USE SampleDB;

CREATE TABLE Employees (
    ID INT PRIMARY KEY,
    Name VARCHAR(50),
    Age INT,
    Salary DECIMAL(18, 2),
    JoinDate DATETIME
);

INSERT INTO Employees (ID, Name, Age, Salary, JoinDate)
VALUES
(1, 'Alice', 30, 60000.00, '2020-01-15'),
(2, 'Bob', 24, 50000.00, '2021-06-01'),
(3, 'Charlie', 29, 55000.00, '2019-11-12'),
(4, 'Diana', 35, 65000.00, '2018-03-23');
