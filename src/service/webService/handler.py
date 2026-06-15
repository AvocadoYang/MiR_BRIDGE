# exceptions.py
import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Union

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute


class CustomSuccessRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            response: Any = await original_route_handler(request)
            success_format = {
                'success': True,
                'code': 200,
                'message': f'Operation successful [{request.method}]',
                'data': None,
            }

            if isinstance(response, Response):
                if 'application/json' in response.headers.get('content-type', ''):
                    try:
                        data = json.loads(response.body.decode('utf-8'))  # type: ignore
                        success_format['data'] = data
                    except Exception:
                        success_format['data'] = response.body.decode('utf-8')  # type: ignore
                else:
                    return response
            else:
                success_format['data'] = response

            return JSONResponse(status_code=200, content=success_format)

        return custom_route_handler


class AppException(Exception):
    """Base exception for all application errors"""

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        error_code: str = 'INTERNAL_ERROR',
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class NotFoundError(AppException):
    """Resource not found"""

    def __init__(self, message: str = ''):
        super().__init__(message=message, status_code=407, error_code='NOTFOUND')


class ValidationError(AppException):
    """Input validation failed"""

    def __init__(self, message: str, value: Any = None):
        super().__init__(
            message=f'Validation error: {message}',
            status_code=422,
            error_code='VALIDATION_ERROR',
            details={'value': str(value) if value else None},
        )


class AuthenticationError(AppException):
    """Authentication failed"""

    def __init__(self, message: str = 'Authentication required'):
        super().__init__(message=message, status_code=401, error_code='AUTHENTICATION_ERROR')


class AuthorizationError(AppException):
    """User lacks permission"""

    def __init__(self, action: str, resource: str, message: Optional[str] = None):
        super().__init__(
            message=message or f'Not authorized to {action} {resource}',
            status_code=403,
            error_code='AUTHORIZATION_ERROR',
            details={'action': action, 'resource': resource},
        )


class ConflictError(AppException):
    """Resource conflict, like duplicate entries"""

    def __init__(self, resource: str, message: Optional[str] = None):
        super().__init__(
            message=message or f'{resource} already exists',
            status_code=409,
            error_code='CONFLICT',
            details={'resource': resource},
        )


class ExternalServiceError(AppException):
    """Third-party service failed"""

    def __init__(self, service: str, message: Optional[str] = None):
        super().__init__(
            message=message or f"External service '{service}' is unavailable",
            status_code=503,
            error_code='SERVICE_UNAVAILABLE',
            details={'service': service},
        )


def create_error_response(
    status_code: int,
    error_code: str,
    message: str,
    details: Union[Dict[str, Any], None] = None,
    request_id: Union[str, None] = None,
) -> Dict[str, Any]:
    """Create a consistent error response structure"""
    response = {
        'success': False,
        'error': {
            'code': error_code,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        },
    }

    if details:
        response['error']['details'] = details

    if request_id:
        response['error']['request_id'] = request_id

    return response
