package handlers

import (
	"encoding/json"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"

	"unbound-future-backend/config"
	"unbound-future-backend/database"
	"unbound-future-backend/models"
)

// FeishuAuthResponse represents the response from Feishu OAuth token endpoint
type FeishuAuthResponse struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	TokenType        string `json:"token_type"`
	ExpiresIn        int    `json:"expires_in"`
	RefreshExpiresIn int    `json:"refresh_expires_in"`
	Scope            string `json:"scope"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// FeishuUserInfo represents the user info from Feishu
type FeishuUserInfo struct {
	Sub       string `json:"sub"` // OpenID
	Name      string `json:"name"`
	Picture   string `json:"picture"`
	Email     string `json:"email"` // Might need scope
	EnName    string `json:"en_name"`
	TenantKey string `json:"tenant_key"`
	AvatarURL string `json:"avatar_url"` // Sometimes different key
}

func GetFeishuLoginURL(c *gin.Context) {
	cfg, err := config.LoadConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load config"})
		return
	}

	// Construct Feishu Login URL
	// https://passport.feishu.cn/suite/passport/oauth/authorize
	authURL := "https://passport.feishu.cn/suite/passport/oauth/authorize"
	u, _ := url.Parse(authURL)
	q := u.Query()
	q.Set("client_id", cfg.Feishu.AppID)
	q.Set("redirect_uri", cfg.Feishu.RedirectURI)
	q.Set("response_type", "code")
	q.Set("state", "unbound_future_login") 
	u.RawQuery = q.Encode()

	c.JSON(http.StatusOK, gin.H{
		"url": u.String(),
	})
}

func FeishuCallback(c *gin.Context) {
	code := c.Query("code")
	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Code is required"})
		return
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load config"})
		return
	}

	// 1. Exchange code for access_token
	tokenURL := "https://passport.feishu.cn/suite/passport/oauth/token"
	vals := url.Values{}
	vals.Set("grant_type", "authorization_code")
	vals.Set("client_id", cfg.Feishu.AppID)
	vals.Set("client_secret", cfg.Feishu.AppSecret)
	vals.Set("code", code)
	vals.Set("redirect_uri", cfg.Feishu.RedirectURI)

	resp, err := http.PostForm(tokenURL, vals)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to connect to Feishu"})
		return
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		c.JSON(resp.StatusCode, gin.H{"error": "Feishu error", "details": string(bodyBytes)})
		return
	}

	var authResp FeishuAuthResponse
	if err := json.Unmarshal(bodyBytes, &authResp); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse Feishu response"})
		return
	}

	if authResp.Error != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": authResp.ErrorDescription})
		return
	}

	// 2. Get User Info
	userInfoURL := "https://passport.feishu.cn/suite/passport/oauth/userinfo"
	req, _ := http.NewRequest("GET", userInfoURL, nil)
	req.Header.Set("Authorization", "Bearer "+authResp.AccessToken)

	client := &http.Client{}
	userResp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch user info"})
		return
	}
	defer userResp.Body.Close()

	userBody, _ := io.ReadAll(userResp.Body)
	var feishuUser FeishuUserInfo
	if err := json.Unmarshal(userBody, &feishuUser); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to parse user info"})
		return
	}

	// 3. Find or Create User in DB
	var user models.User
	result := database.DB.Where("feishu_open_id = ?", feishuUser.Sub).First(&user)

	if result.Error == gorm.ErrRecordNotFound {
		// Create new user
		user = models.User{
			FeishuOpenID: feishuUser.Sub,
			Name:         feishuUser.Name,
			Email:        feishuUser.Email,
			AvatarURL:    feishuUser.Picture,
		}
		if user.AvatarURL == "" {
			user.AvatarURL = feishuUser.AvatarURL
		}

		if err := database.DB.Create(&user).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user"})
			return
		}
	} else if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	} else {
		// Update user info
		user.Name = feishuUser.Name
		user.AvatarURL = feishuUser.Picture
		if user.AvatarURL == "" {
			user.AvatarURL = feishuUser.AvatarURL
		}
		database.DB.Save(&user)
	}

	// 4. Generate JWT
	tokenString, err := generateJWT(user, cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	userData := gin.H{
		"id":     user.ID,
		"name":   user.Name,
		"avatar": user.AvatarURL,
		"email":  user.Email,
	}
	userBytes, _ := json.Marshal(userData)
	userStr := base64.URLEncoding.EncodeToString(userBytes)

	// Redirect to frontend
	// Assuming frontend is served from the same domain or we know the host.
	// We can use the Referer or a configured Frontend URL.
	// For now, let's assume relative redirect works if served from same domain via Nginx
	// Or use /auth/finish
	c.Redirect(http.StatusFound, "/auth/finish?access_token="+tokenString+"&user="+userStr)
}

func DebugLogin(c *gin.Context) {
	cfg, err := config.LoadConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to load config"})
		return
	}

	// 1. 获取用户提交的密码/凭证
	var req struct {
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	// 2. 简单的硬编码鉴权
	if req.Password != "jaime123" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid password"})
		return
	}

	// 3. 构造或获取用户
	// Hardcoded user for debug
	user := models.User{
		FeishuOpenID: "debug_jaime123",
		Name:         "jaime123",
		Email:        "jaime123@debug.local",
		AvatarURL:    "https://ui-avatars.com/api/?name=jaime123",
	}

	// Upsert user
	var existingUser models.User
	result := database.DB.Where("feishu_open_id = ?", user.FeishuOpenID).First(&existingUser)
	if result.Error == gorm.ErrRecordNotFound {
		if err := database.DB.Create(&user).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create user"})
			return
		}
	} else {
		user = existingUser
	}

	// Generate JWT
	tokenString, err := generateJWT(user, cfg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"access_token": tokenString,
		"user": gin.H{
			"id":     user.ID,
			"name":   user.Name,
			"avatar": user.AvatarURL,
			"email":  user.Email,
		},
	})
}

func generateJWT(user models.User, cfg *config.Config) (string, error) {
	claims := jwt.MapClaims{
		"sub": user.ID,
		"exp": time.Now().Add(time.Duration(cfg.Security.AccessTokenExpireMins) * time.Minute).Unix(),
		"iat": time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(cfg.Security.JWTSecret))
}
