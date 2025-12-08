# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Loading pattern helpers for long-running views.

Provides consistent loading page patterns with optional extended timeout handling.
"""
from django.http import HttpResponse, JsonResponse
from django.utils.html import escape


def create_response_with_loading(
    request,
    task_title,
    success_message,
    return_url,
    return_link_text,
    extended=False,
    timeout_ms=300000
):
    """
    Create a loading page response with AJAX task execution.

    Args:
        request: Django HTTP request object
        task_title: Title displayed during loading
        success_message: Message shown on success
        return_url: URL for the return link
        return_link_text: Text for the return link
        extended: Use extended version with error display and timeout handling
        timeout_ms: Timeout in milliseconds for extended mode (default: 5 minutes)

    Returns:
        HttpResponse with loading page HTML
    """
    # Escape all user-influenced arguments before insertion into HTML
    safe_task_title = escape(task_title)
    safe_success_message = escape(success_message)
    safe_return_url = escape(return_url)
    safe_return_link_text = escape(return_link_text)

    # Extended version includes error display and timeout handling
    error_message_html = ""
    error_handler_js = ""
    timeout_handler_js = ""
    progress_text = ""
    delay_ms = 100

    if extended:
        error_message_html = f'''
                <div id="error-message">
                    <p><strong>Error:</strong> <span id="error-text"></span></p>
                    <p>Please check the server logs for more details.</p>
                    <p>Go back to <a href="{safe_return_url}">{safe_return_link_text}</a></p>
                </div>'''

        error_handler_js = '''
                        .catch(error => {
                            clearTimeout(timeoutId);
                            console.error('Error:', error);
                            document.getElementById('loading-overlay').style.display = 'none';
                            document.getElementById('error-text').textContent = error.message;
                            document.getElementById('error-message').style.display = 'block';
                        });'''

        timeout_handler_js = f'''
                        const controller = new AbortController();
                        const timeoutId = setTimeout(() => controller.abort(), {timeout_ms});'''

        progress_text = '''<div class="progress-text">This process may take several minutes. Please do not close this window.</div>'''
        delay_ms = 100  # Extended mode uses small delay
    else:
        error_handler_js = '''
                        .catch(error => {
                            console.error('Error:', error);
                            alert('An error occurred while processing the task: ' + error.message);
                        });'''
        delay_ms = 100

    error_style = ""
    if extended:
        error_style = '''
                #error-message {
                    display: none;
                    margin-top: 20px;
                    padding: 15px;
                    background-color: #f8d7da;
                    border: 1px solid #f5c6cb;
                    border-radius: 4px;
                    color: #721c24;
                }

                .progress-text {
                    margin-top: 10px;
                    font-size: 14px;
                    color: #666;
                }'''

    init_error_display = ""
    if extended:
        init_error_display = "document.getElementById('error-message').style.display = 'none';"

    fetch_options = "method: 'GET', headers: {'X-Requested-With': 'XMLHttpRequest'}"
    if extended:
        fetch_options += ", signal: controller.signal"

    response_handler = ""
    if extended:
        response_handler = '''
                            clearTimeout(timeoutId);
                            if (!response.ok) {
                                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                            }'''

    success_content_update = ""
    if extended:
        success_content_update = f'''
                                const successDiv = document.getElementById('success-message');
                                let successContent = '<p>{safe_success_message}</p>';
                                if (data.instructions) {{
                                    successContent += '<div style="margin-top: 15px; padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 4px;">';
                                    successContent += '<h4 style="margin-top: 0; color: #856404;">Next Steps:</h4>';
                                    successContent += '<ol style="margin-bottom: 0;">';
                                    data.instructions.forEach(instruction => {{
                                        successContent += '<li style="margin-bottom: 5px;">' + instruction + '</li>';
                                    }});
                                    successContent += '</ol></div>';
                                }}
                                successContent += '<p>Go back to <a href="{safe_return_url}">{safe_return_link_text}</a></p>';
                                successDiv.innerHTML = successContent;
                                successDiv.style.display = 'block';'''

    html_response = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                .loading-overlay {{
                    position: fixed;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(255, 255, 255, 0.8);
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    flex-direction: column;
                    z-index: 9999;
                }}

                .loading-spinner {{
                    width: 50px;
                    height: 50px;
                    border: 5px solid #f3f3f3;
                    border-top: 5px solid #3498db;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                    margin-bottom: 20px;
                }}

                @keyframes spin {{
                    0% {{ transform: rotate(0deg); }}
                    100% {{ transform: rotate(360deg); }}
                }}

                .loading-message {{
                    font-size: 18px;
                    color: #333;
                    {"text-align: center; max-width: 500px;" if extended else ""}
                }}

                .task-info {{
                    padding: 20px;
                    max-width: 600px;
                    margin: 0 auto;
                }}

                #success-message {{
                    display: none;
                    margin-top: 20px;
                    padding: 15px;
                    background-color: #d4edda;
                    border: 1px solid #c3e6cb;
                    border-radius: 4px;
                    color: #155724;
                }}
                {error_style}
            </style>
        </head>
        <body>
            <div class="task-info">
                <h3>{safe_task_title}</h3>
                <div id="loading-overlay" class="loading-overlay">
                    <div class="loading-spinner"></div>
                    <div class="loading-message">
                        Please wait while the task completes...{"<br>" + progress_text if extended else ""}
                    </div>
                </div>
                <div id="success-message">
                    <p>{safe_success_message}</p>
                    <p>Go back to <a href="{safe_return_url}">{safe_return_link_text}</a></p>
                </div>
                {error_message_html}
            </div>
            <script>
                document.addEventListener('DOMContentLoaded', function() {{
                    document.getElementById('loading-overlay').style.display = 'flex';
                    document.getElementById('success-message').style.display = 'none';
                    {init_error_display}

                    setTimeout(() => {{
                        {timeout_handler_js if extended else ""}
                        fetch(window.location.href + '?execute=true', {{
                            {fetch_options}
                        }})
                        .then(response => {{
                            {response_handler}
                            return response.json();
                        }})
                        .then(data => {{
                            if (data.status === 'success') {{
                                document.getElementById('loading-overlay').style.display = 'none';
                                {"" if extended else "document.getElementById('success-message').style.display = 'block';"}
                                {success_content_update if extended else ""}
                            }} else {{
                                throw new Error(data.message || 'Task failed');
                            }}
                        }})
                        {error_handler_js}
                    }}, {delay_ms});
                }});
            </script>
        </body>
        </html>
    """

    # If this is the AJAX request to execute the task
    if request.GET.get('execute') == 'true':
        try:
            return JsonResponse({'status': 'success'})
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            return SecureErrorHandler.secure_json_response(e, "task execution", request)

    return HttpResponse(html_response)


def create_response_with_loading_extended(request, task_title, success_message, return_url, return_link_text):
    """
    Extended version of create_response_with_loading with better timeout handling.

    This is a convenience wrapper for backward compatibility.
    """
    return create_response_with_loading(
        request,
        task_title,
        success_message,
        return_url,
        return_link_text,
        extended=True
    )
