# OneWorldDbClient

Documentation on using OneWorld Db Client is not available currently, sorry.

[![Build Status](https://dev.azure.com/SiroccoHub/OneWorldDbClient/_apis/build/status/SiroccoHub.OneWorldDbClient?branchName=master)](https://dev.azure.com/SiroccoHub/OneWorldDbClient/_build/latest?definitionId=6&branchName=master)

## What is OneWorldDbClient?

OneWorldDbClient is a DI Component for .NET Core / ASP NET Core for using micro-ORM (like Dapper)  with  EF Core together under combined DB Transaction.

By using OneWorldDbClient, you can safely share and use DB connections and transactions across multiple DI instances.

## Why is OneWorldDbClient?

<https://medium.com/team-sirocco-publications/approach-of-db-transactions-using-asp-net-core-ef-core-orm-like-dapper-and-di-implementation-bf9a72b85237>

## Getting Started

There are currently two Nuget packages - the core library (OneWorldDbClient) and a extension package for Microsoft SQL Server (OneWorldDbClient.SqlServer).

### NuGet Installation

* OneWorldDbClient
  * <https://www.nuget.org/packages/OneWorldDbClient/>  
* OneWorldDbClient.SqlServer
  * <https://www.nuget.org/packages/OneWorldDbClient.SqlServer/>

Simply installing the following Nuget package:

    Install-Package OneWorldDbClient
    Install-Package OneWorldDbClient.SqlServer

### How to use in ASP NET Core

<https://medium.com/team-sirocco-publications/approach-of-db-transactions-using-asp-net-core-ef-core-orm-like-dapper-and-di-implementation-bf9a72b85237>

### History

#### v0.1.0
+ First release.

#### v0.2.0
+ Breaking chagne: Forgetting to vote is abnormal and throws an `InvalidOperationException()`.
