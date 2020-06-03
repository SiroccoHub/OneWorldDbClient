# OneWorldDbClient

Documentation on using OneWorld Db Client is not available currently, sorry.

![Run Build with Test](https://github.com/SiroccoHub/OneWorldDbClient/workflows/Run%20Build%20with%20Test/badge.svg)

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

#### v1.1.0
+ Fix invalid log comment.
+ Fix the `first transaction`'s behavior.
  + The first `RequresNew` Tx has the *own scope*, The first `Requred` Tx has the *manager scope*. 

#### v1.0.0
+ Change Dependencies.
  + `Microsoft.Data.SqlClient` is used instead of `System.Data.SqlClient`
    + https://docs.microsoft.com/en-us/ef/core/what-is-new/ef-core-3.0/breaking-changes#SqlClient
+ Update Dependencies.

#### v0.2.0
+ Breaking chagne: Forgetting to vote is abnormal and throws an `InvalidOperationException()`.

#### v0.1.0
+ First release.
