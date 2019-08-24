FROM mcr.microsoft.com/dotnet/core/aspnet:3.0-buster-slim AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/core/sdk:3.0-buster AS build
WORKDIR /src
COPY ["src/OneWorldDbClient.SampleWeb/OneWorldDbClient.SampleWeb.csproj", "src/OneWorldDbClient.SampleWeb/"]
RUN dotnet restore "src/OneWorldDbClient.SampleWeb/OneWorldDbClient.SampleWeb.csproj"
COPY . .
WORKDIR "/src/src/OneWorldDbClient.SampleWeb"
RUN dotnet build "OneWorldDbClient.SampleWeb.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "OneWorldDbClient.SampleWeb.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OneWorldDbClient.SampleWeb.dll"]