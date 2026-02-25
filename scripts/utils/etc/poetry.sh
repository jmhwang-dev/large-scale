curl -sSL https://install.python-poetry.org | python3 -

echo 'add_to_path "$HOME/.local/bin"' > ~/.envs/poetry.sh

# poetry install # 파이썬 버전 필요 (conda 로 설치해서 파이썬 버전 설정 가능)
# poetry shell

# poetry env remove python
# poetry env use path_python

# or
# poetry env list # 가상환경확인
# poetry env remove <가상환경 이름 또는 경로>

# poetry update or  poetry lock
# | 항목      | `poetry lock`                 | `poetry update`        |
# | ------- | ----------------------------- | ---------------------- |
# | 목적      | 현재 요구사항 기준으로 **lock 파일만 재작성** | **의존성 최신화 + lock 재작성** |
# | 설치되는 버전 | 기존 lock 파일에 최대한 **가까운 버전**    | 가능한 **최신 버전 설치**       |
# | 사용 시점   | `pyproject.toml`만 변경됐을 때      | 의존성 최신 상태로 만들고 싶을 때    |

# poetry install
