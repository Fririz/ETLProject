FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["ETLProject.ConsoleApp/ETLProject.ConsoleApp.csproj", "ETLProject.ConsoleApp/"]
RUN dotnet restore "ETLProject.ConsoleApp/ETLProject.ConsoleApp.csproj"
COPY . .
WORKDIR "/src/ETLProject.ConsoleApp"
RUN dotnet build "./ETLProject.ConsoleApp.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./ETLProject.ConsoleApp.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ETLProject.ConsoleApp.dll"]
