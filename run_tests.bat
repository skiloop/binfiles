@echo off
REM Test runner script for binfiles project (Windows)
REM Usage: run_tests.bat [unit|functional|performance|all]

setlocal enabledelayedexpansion

if "%1"=="help" goto :help
if "%1"=="--help" goto :help
if "%1"=="-h" goto :help
if "%1"=="" goto :all
if "%1"=="all" goto :all
if "%1"=="unit" goto :unit
if "%1"=="functional" goto :functional
if "%1"=="performance" goto :performance
goto :unknown

:unit
echo [INFO] Running unit tests...
cd binfile\test\unit
go test -v -race -coverprofile=coverage.out .
if %errorlevel% neq 0 (
    echo [ERROR] Unit tests failed
    exit /b 1
)
echo [SUCCESS] Unit tests passed
go tool cover -html=coverage.out -o coverage.html
echo [INFO] Coverage report generated: coverage.html
cd ..\..\..
goto :end

:functional
echo [INFO] Running functional tests...
cd binfile\test\functional
go test -v -timeout=10m .
if %errorlevel% neq 0 (
    echo [ERROR] Functional tests failed
    exit /b 1
)
echo [SUCCESS] Functional tests passed
cd ..\..\..
goto :end

:performance
echo [INFO] Running performance tests...
cd binfile\test\performance
go test -v -bench=. -benchmem .
if %errorlevel% neq 0 (
    echo [ERROR] Performance tests failed
    exit /b 1
)
echo [SUCCESS] Performance tests passed
cd ..\..\..
goto :end

:all
echo [INFO] Running all tests...

echo [INFO] Running unit tests...
cd binfile\test\unit
go test -v -race -coverprofile=coverage.out .
if %errorlevel% neq 0 (
    echo [ERROR] Unit tests failed, stopping
    exit /b 1
)
echo [SUCCESS] Unit tests passed
go tool cover -html=coverage.out -o coverage.html
echo [INFO] Coverage report generated: coverage.html
cd ..\..\..

echo [INFO] Running functional tests...
cd binfile\test\functional
go test -v -timeout=10m .
if %errorlevel% neq 0 (
    echo [ERROR] Functional tests failed, stopping
    exit /b 1
)
echo [SUCCESS] Functional tests passed
cd ..\..\..

echo [INFO] Running performance tests...
cd binfile\test\performance
go test -v -bench=. -benchmem .
if %errorlevel% neq 0 (
    echo [ERROR] Performance tests failed, stopping
    exit /b 1
)
echo [SUCCESS] Performance tests passed
cd ..\..\..

echo [SUCCESS] All tests passed!
goto :end

:help
echo Usage: %0 [unit^|functional^|performance^|all]
echo.
echo Commands:
echo   unit        Run unit tests only
echo   functional  Run functional tests only
echo   performance Run performance tests only
echo   all         Run all tests (default)
echo   help        Show this help message
echo.
echo Examples:
echo   %0 unit
echo   %0 functional
echo   %0 performance
echo   %0 all
goto :end

:unknown
echo [ERROR] Unknown command: %1
goto :help

:end
endlocal
