#!/bin/bash
export PATH=$PATH:/opt/keycloak/bin

sleep 30
while ! kcadm.sh config credentials --server http://localhost:8080 --realm master --user admin --password admin; do
    sleep 1
done

# Add users to Keycloak
for user in alice bob carol oscar; do
  kcadm.sh create users -r master -s username="$user" -s enabled=true
  kcadm.sh set-password -r master --username "$user" --new-password "$user"
done

allowed_protocol_mappers=$(kcadm.sh get components -q name="Allowed Protocol Mapper Types" --fields id --format csv --noquotes)

for i in $allowed_protocol_mappers;do
  kcadm.sh update components/$i -s 'config.allowed-protocol-mapper-types=[ "saml-user-attribute-mapper", "saml-user-property-mapper", "oidc-usermodel-property-mapper", "oidc-usermodel-attribute-mapper", "oidc-full-name-mapper", "oidc-address-mapper", "oidc-audience-mapper", "oidc-sha256-pairwise-sub-mapper", "saml-role-list-mapper" ]'
done

kcreg.sh config credentials --server http://localhost:8080 --realm master --user admin --password admin

for client in tiled-cli.json tiled.json; do
    kcreg.sh create --file "/mnt/$client"
done
