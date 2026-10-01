FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY *.csproj ./
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
# Нужен доступ к /var/run/docker.sock (управление контейнерами) — в учебном стенде запускаем от root.
USER root
EXPOSE 8080
ENTRYPOINT ["dotnet", "ControlCenter.dll"]
