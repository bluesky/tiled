package token

issuer := opa.runtime().env.ISSUER

jwks_endpoint := jwks_endpoint if {
	metadata := http.send({
		"url": concat("", [issuer, "/.well-known/openid-configuration"]),
		"method": "GET",
		"force_cache": true,
		"force_cache_duration_seconds": 86400,
	}).body
	jwks_endpoint := metadata.jwks_uri
}

fetch_jwks(url) := http.send({
	"url": url,
	"method": "GET",
	"force_cache": true,
	"force_cache_duration_seconds": 86400,
})

unverified := io.jwt.decode(input.token)

jwt_header := unverified[0]

jwks_url := concat("?", [jwks_endpoint, urlquery.encode_object({"kid": jwt_header.kid})])

jwks := fetch_jwks(jwks_url).raw_body

verified := io.jwt.decode_verify(input.token, {
	"cert": jwks,
	"iss": issuer,
	"aud": input.audience,
})

claims := verified[2] if verified[0]

roles := claims.realm_access.roles
name := claims.preferred_username
