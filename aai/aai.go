// Copyright 2021 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aai

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Nerzal/gocloak/v8"
	"github.com/pkg/errors"
)

type UserInfo struct {
	Sub               string              `json:"sub,omitempty"`
	Name              string              `json:"name,omitempty"`
	GivenName         string              `json:"given_name,omitempty"`
	FamilyName        string              `json:"family_name,omitempty"`
	MiddleName        string              `json:"middle_name,omitempty"`
	Nickname          string              `json:"nickname,omitempty"`
	PreferredUsername string              `json:"preferred_username,omitempty"`
	Profile           string              `json:"profile,omitempty"`
	Picture           string              `json:"picture,omitempty"`
	Website           string              `json:"website,omitempty"`
	Email             string              `json:"email,omitempty"`
	Gender            string              `json:"gender,omitempty"`
	ZoneInfo          string              `json:"zoneinfo,omitempty"`
	Locale            string              `json:"locale,omitempty"`
	PhoneNumber       string              `json:"phone_number,omitempty"`
	Attributes        map[string][]string `json:"attributes,omitempty"`
}

func (u UserInfo) GetName() string {
	if u.PreferredUsername != "" {
		return u.PreferredUsername
	}
	if u.Email != "" {
		return u.Email
	}

	return u.GivenName
}

// Client is the client interface to HEAppE service
type Client interface {
	// ExchangeToken exchanges a token to get an access and a refresh token for this client
	ExchangeToken(ctx context.Context, accessToken string) (string, string, error)
	// IsAccessTokenValid checks if an access token is still valid
	IsAccessTokenValid(ctx context.Context, accessToken string) (bool, error)
	// GetUserInfo returns info on the user (name, attributes, etc..)
	GetUserInfo(ctx context.Context, accessToken string) (UserInfo, error)
	// RefreshToken refreshes a token to get a new access and a refresh token for this client
	RefreshToken(ctx context.Context, refreshToken string) (string, string, error)
}

// GetClient returns a client of the Authentication and Authorization Infrastructure service
func GetClient(url, clientID, clientSecret, realm string) Client {
	keycloakClient := gocloak.NewClient(url)
	aaiClient := aaiClient{
		keycloak:     keycloakClient,
		clientID:     clientID,
		clientSecret: clientSecret,
		realm:        realm,
		url:          url,
	}
	return &aaiClient
}

type aaiClient struct {
	keycloak     gocloak.GoCloak
	clientID     string
	clientSecret string
	realm        string
	url          string
}

// IsAccessTokenValid checks if an access token is still valid
func (c *aaiClient) IsAccessTokenValid(ctx context.Context, accessToken string) (bool, error) {
	res, err := c.keycloak.RetrospectToken(ctx, accessToken, c.clientID, c.clientSecret, c.realm)
	if err != nil {
		return false, err
	}

	return *res.Active, err
}

// GetUserInfo returns info on the user (name, attributes, etc..)
func (c *aaiClient) GetUserInfo(ctx context.Context, accessToken string) (UserInfo, error) {

	var userInfo UserInfo
	res, err := c.keycloak.GetRawUserInfo(ctx, accessToken, c.realm)
	if err != nil {
		return userInfo, err
	}

	body, err := json.Marshal(res)
	if err != nil {
		return userInfo, err
	}

	err = json.Unmarshal(body, &userInfo)
	if err != nil {
		err = errors.Wrapf(err, "Failed to unmarshall %s", string(body))
	}
	return userInfo, err
}

// ExchangeToken exchanges a token to get an access and a refresh token for this client
func (c *aaiClient) ExchangeToken(ctx context.Context, accessToken string) (string, string, error) {
	var result gocloak.JWT
	res, err := c.keycloak.RestyClient().R().SetFormData(
		map[string]string{
			"client_id":            c.clientID,
			"client_secret":        c.clientSecret,
			"grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
			"subject_token":        accessToken,
			"subject_token_type":   "urn:ietf:params:oauth:token-type:access_token",
			"requested_token_type": "urn:ietf:params:oauth:token-type:refresh_token",
			"scope":                "offline_access",
		}).SetResult(&result).Post(fmt.Sprintf("%s/auth/realms/%s/protocol/openid-connect/token", c.url, c.realm))

	if err != nil {
		return "", "", err
	}

	if res == nil {
		return "", "", errors.Errorf("Unexpected empty response exchaning a token")
	}

	if res.IsError() {
		var errMsg string
		if e, ok := res.Error().(*gocloak.HTTPErrorResponse); ok && e.NotEmpty() {
			errMsg = fmt.Sprintf("%s: %s", res.Status(), e)
		} else {
			errMsg = res.Status()
		}

		return "", "", errors.Errorf(errMsg)
	}

	return result.AccessToken, result.RefreshToken, err
}

// RefreshToken refreshes a token to get a new access and a refresh token for this client
func (c *aaiClient) RefreshToken(ctx context.Context, refreshToken string) (string, string, error) {

	res, err := c.keycloak.RefreshToken(ctx, refreshToken, c.clientID, c.clientSecret, c.realm)
	if err != nil {
		return "", "", err
	}

	return res.AccessToken, res.RefreshToken, err
}
