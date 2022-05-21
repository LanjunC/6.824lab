@REM https://blog.csdn.net/qq_33811402/article/details/51774287
setlocal enabledelayedexpansion
for /l %%i in (1,1,10) do (
    go test -run 3A >> tmplog%%i.txt
    if !errorlevel! == 0 (
        del .\tmplog%%i.txt
    )
)
