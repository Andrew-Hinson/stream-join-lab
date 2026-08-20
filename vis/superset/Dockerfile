FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir trino
COPY assets /tmp/assets
COPY docker-init.sh /app/docker-init.sh
COPY superset_config.py /app/pythonpath/superset_config.py
RUN python -c "\
import os, zipfile;\
root='/tmp/assets';\
z=zipfile.ZipFile('/app/match-facts.zip','w',zipfile.ZIP_DEFLATED);\
[z.write(os.path.join(dp,f), os.path.join('dashboard_export', os.path.relpath(os.path.join(dp,f), root))) for dp,_,fs in os.walk(root) for f in fs];\
z.close()" \
 && chmod +x /app/docker-init.sh \
 && chown superset:superset /app/match-facts.zip /app/docker-init.sh /app/pythonpath/superset_config.py
USER superset
