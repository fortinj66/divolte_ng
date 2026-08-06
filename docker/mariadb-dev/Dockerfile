# Throwaway dev/test MariaDB - backs internal/store during development
# only (see docs/admin-ui-guide.md). The real shared instance production
# instances point at is provisioned separately, not from this image.
FROM fedora:44

RUN dnf install -y mariadb-server && dnf clean all

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

VOLUME /var/lib/mysql
EXPOSE 3306

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
