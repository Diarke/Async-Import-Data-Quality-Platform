from fastapi import status, HTTPException


file_isnt_suported = HTTPException(
    status_code=status.HTTP_400_BAD_REQUEST,
    detail="Данный файл не поддерживается, файл должен иметь другое окончания"
)
