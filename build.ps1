
# STEP 1: Determinate the required values

param(
    [Parameter(Position=0)]
    [String]$os = "windows"
)

$env:PACKAGE="github.com/skiloop/binfiles"
$env:VERSION="$(git describe --tags --always --abbrev=0 --match='v[0-9]*.[0-9]*.[0-9]*' 2> $null | foreach {$_ -replace('^v', '')})"
$env:COMMIT_HASH="$(git rev-parse --short HEAD)"
$env:BUILD_TIMESTAMP="$(GET-DATE)"

echo "VERSION         : $env:VERSION"
echo "COMMIT_HASH     : $env:COMMIT_HASH"
echo "BUILD_TIMESTAMP : $env:BUILD_TIMESTAMP"
echo "TARGET OS       : $os"
# STEP 2: Build the ldflags

$env:LDFLAGS=@(
  "-X '$env:PACKAGE/version.Version=$env:VERSION'"
  "-X '$env:PACKAGE/version.CommitHash=$env:COMMIT_HASH'"
  "-X '$env:PACKAGE/version.BuildTime=$env:BUILD_TIMESTAMP'"
  )


# STEP 3: Actual Go build process
$env:CGO_ENABLED=0
if ('linux' -eq $os) {
    $env:GOOS = 'linux'
    $env:GOARCH = 'amd64'
    $env:TARGET = 'binutil'
}else{
    $env:GOOS = 'windows'
    $env:GOARCH = 'amd64'
    $env:TARGET = 'binutil.exe'
}
go build -ldflags="$env:LDFLAGS" -o $env:TARGET