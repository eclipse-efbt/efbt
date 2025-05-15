"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
from datetime import datetime

"""
!todo():
    show more basic view of the test (table test to for datapoint that have test)

"""

def generate_technical_test_report(json_data):
    html_template = """
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JSON Data Summary</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            padding: 20px;
            background-color: #f4f4f4;
        }}
        .container {{
            max-width: 800px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
            margin: 0 auto;
        }}
        h2 {{
            color: #333;
            border-bottom: 2px solid #ddd;
            padding-bottom: 5px;
        }}
        .section {{
            margin-bottom: 20px;
        }}
        .key {{
            font-weight: bold;
            color: #555;
        }}
        .value {{
            color: #777;
        }}
        ul {{
            list-style-type: none;
            padding: 0;
        }}
        li {{
            background: #eef;
            margin: 5px 0;
            padding: 8px;
            border-radius: 5px;
        }}
        .passed-test {{
            background: #e6ffe6;
        }}
        .failed-test {{
            background: #ffe6e6;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Data Summary</h1>

        <div class="section">
            <h2>Datapoint Information</h2>
            <p><span class="key">Datapoint Value:</span> {datapoint_value}</p>
            <p><span class="key">Regulatory Template ID:</span> {regulatory_template_id}</p>
            <p><span class="key">Datapoint Suffix:</span> {datapoint_suffix}</p>
        </div>

        <div class="section">
            <h2>System Information</h2>
            <p><span class="key">Operating System:</span> {os}</p>
            <p><span class="key">Python Path:</span> {python_path}</p>
            <h3>Installed Packages:</h3>
            <ul>
                {packages}
            </ul>
        </div>

        <div class="section">
            <h2>Project Configuration</h2>
            <p><span class="key">Cache Directory:</span> {cachedir}</p>
            <p><span class="key">Root Directory:</span> {rootdir}</p>
            <p><span class="key">Config File:</span> {configfile}</p>
        </div>

        <div class="section">
            <h2>Test Results</h2>
            <h3>Passed Tests:</h3>
            <ul>
                {passed_tests}
            </ul>
            <h3>Failed Tests:</h3>
            <ul>
                {failed_tests}
            </ul>
        </div>
    </div>
</body>
</html>"""

    # Read JSON data
    packages_html = "\n                ".join([f"<li>{pkg}</li>" for pkg in json_data["platform_info"]["packages"]])
    passed_tests_html = "\n                ".join([f"<li class='passed-test'>{test}</li>" for test in json_data["test_results"]["passed"]])
    failed_tests = f"<li class='passed-test'>{"None"}</li>" if not json_data["test_results"]["failed"] else "\n".join([f"<li class='failed-test'>{test}</li>" for test in json_data["test_results"]["failed"]])

    # Format HTML
    html_content = html_template.format(
        datapoint_value=json_data["test_information"]["datapoint_value"],
        regulatory_template_id=json_data["test_information"]["regulatory_template_id"],
        datapoint_suffix=json_data["test_information"]["datapoint_suffix"],
        os=json_data["platform_info"]["os"].capitalize(),
        python_path=json_data["platform_info"]["python"],
        packages=packages_html,
        cachedir=json_data["paths"]["cachedir"],
        rootdir=json_data["paths"]["rootdir"],
        configfile=json_data["paths"]["configfile"],
        passed_tests=passed_tests_html,
        failed_tests=failed_tests
    )

    return html_content
