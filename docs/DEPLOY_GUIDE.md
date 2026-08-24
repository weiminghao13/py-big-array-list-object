# 域名 + HTTPS 部署指南

把你的 BigArrayList 文档网站挂上自己的域名，全程免费托管 + 自动 HTTPS。

## 最终效果

- 网址：`https://www.pybigarraylist.com`
- 协议：HTTPS（小绿锁）
- 托管：GitHub Pages（免费）
- 域名：`.com` 一年约 55~70 元

---

## 第一步：注册域名

### 选注册商（三选一）

| 注册商 | 首年价格 | 优点 | 缺点 |
|--------|---------|------|------|
| 阿里云（万网） | ~55 元/年 | 国内访问快、实名认证方便 | 需要身份证实名 |
| 腾讯云 | ~58 元/年 | 界面友好、新用户优惠 | 需要身份证实名 |
| Namecheap | ~60 元/年 | 不需要实名、隐私保护免费 | 英文界面、支付需信用卡/PayPal |

**推荐用阿里云**，国内访问最稳。

### 操作步骤

1. 打开 [阿里云域名注册](https://wanwang.aliyun.com/)
2. 搜索框输入 `pybigarraylist`，点"查域名"
3. 找到 `.com` 那一行，点"加入清单"
4. 去结算，付款（首年约 55 元）
5. 付款后会提示"实名认证"，按提示上传身份证，等审核通过（通常几分钟到几小时）

> 如果 `pybigarraylist.com` 被人注册了，可以换：`pybigarraylist.dev`（强制 HTTPS）、`pybigarraylist.io`、`bigarraylist.cn` 等。

---

## 第二步：把代码推到 GitHub

如果你还没有 GitHub 仓库：

1. 注册 [GitHub](https://github.com) 账号
2. 点右上角 `+` → `New repository`
3. 仓库名填 `PyBigArrayListObject`（或你喜欢的名字）
4. 选 `Public`（公开，GitHub Pages 免费）
5. 不要勾选 "Add a README"（你本地已经有了）
6. 点 `Create repository`

然后在本地项目目录执行：

```powershell
cd C:\Users\浩浩\Desktop\PyBigArrayListObject
git init
git add .
git commit -m "init: BigArrayList project with docs site"
git branch -M main
git remote add origin https://github.com/你的用户名/PyBigArrayListObject.git
git push -u origin main
```

---

## 第三步：开启 GitHub Pages

1. 打开你的 GitHub 仓库页面
2. 点顶部 `Settings`（设置）
3. 左侧菜单找到 `Pages`（在 Code and automation 分类下）
4. `Source` 选 `Deploy from a branch`
5. `Branch` 选 `main`，文件夹选 `/docs`
6. 点 `Save`

等 1~2 分钟，页面上方会出现一个绿色提示：
> Your site is live at `https://你的用户名.github.io/PyBigArrayListObject/`

点这个链接，确认网站能正常打开。

---

## 第四步：配置 DNS 解析

回到阿里云（或你买域名的地方）：

1. 进入 [阿里云域名控制台](https://dc.console.aliyun.com/)
2. 找到你的域名，点右侧 `解析`
3. 点 `添加记录`，添加以下 **5 条记录**：

### 记录 1：根域名 A 记录（第 1 个 IP）

| 字段 | 值 |
|------|-----|
| 记录类型 | `A` |
| 主机记录 | `@` |
| 记录值 | `185.199.108.153` |
| TTL | `10 分钟`（默认就行） |

### 记录 2：根域名 A 记录（第 2 个 IP）

| 字段 | 值 |
|------|-----|
| 记录类型 | `A` |
| 主机记录 | `@` |
| 记录值 | `185.199.109.153` |

### 记录 3：根域名 A 记录（第 3 个 IP）

| 字段 | 值 |
|------|-----|
| 记录类型 | `A` |
| 主机记录 | `@` |
| 记录值 | `185.199.110.153` |

### 记录 4：根域名 A 记录（第 4 个 IP）

| 字段 | 值 |
|------|-----|
| 记录类型 | `A` |
| 主机记录 | `@` |
| 记录值 | `185.199.111.153` |

### 记录 5：www 子域名 CNAME 记录

| 字段 | 值 |
|------|-----|
| 记录类型 | `CNAME` |
| 主机记录 | `www` |
| 记录值 | `你的用户名.github.io` |

> 注意：`你的用户名.github.io` 后面**不要加**仓库名，也不要加 `https://`。

添加完后，等 DNS 生效（通常 5~30 分钟，最慢 24 小时）。

可以在电脑上验证：

```powershell
nslookup www.pybigarraylist.com
```

如果返回的是 GitHub 的 IP，说明 DNS 生效了。

---

## 第五步：在 GitHub 绑定自定义域名

1. 回到 GitHub 仓库 → `Settings` → `Pages`
2. 在 `Custom domain` 输入框里填：`www.pybigarraylist.com`
3. 点 `Save`
4. GitHub 会自动检查 DNS 配置，检查通过后会显示绿色对勾

> 项目里的 `docs/CNAME` 文件已经写好了 `www.pybigarraylist.com`，GitHub 会自动读取它。如果你改了域名，记得同步改这个文件。

---

## 第六步：开启 HTTPS（小绿锁）

1. 还是在 GitHub Pages 设置页面
2. 往下滚，找到 `Enforce HTTPS` 选项
3. 勾选它（如果是灰色不可点，等 5~10 分钟让 GitHub 签发证书，刷新页面再勾）

勾选后，访问 `http://www.pybigarraylist.com` 会自动跳转到 `https://www.pybigarraylist.com`。

---

## 验证清单

- [ ] `https://www.pybigarraylist.com` 能打开首页
- [ ] 浏览器地址栏有小绿锁（HTTPS）
- [ ] 点"API 参考文档"能跳转到 `api_reference_interactive.html`
- [ ] 点"架构图"能跳转到 `architecture_diagram.html`
- [ ] `http://` 自动跳转到 `https://`
- [ ] `pybigarraylist.com`（不带 www）也能访问（会跳转到 www）

---

## 常见问题

### Q: DNS 配置了但 GitHub 显示 DNS 检查失败？
A: 等。DNS 传播需要时间，最多 24 小时。可以用 `nslookup www.pybigarraylist.com` 检查。

### Q: `Enforce HTTPS` 是灰色的勾不了？
A: GitHub 正在给你签发 SSL 证书，等 5~10 分钟刷新页面。

### Q: 网站打开是 404？
A: 检查 GitHub Pages 的 Branch 选的是不是 `main` + `/docs` 文件夹。

### Q: 我想用别的域名怎么办？
A: 改三个地方：① `docs/CNAME` 文件里的域名；② GitHub Pages 设置里的 Custom domain；③ DNS 解析记录不用改（A 记录和 CNAME 都是通用的）。

### Q: 每年要花多少钱？
A: 只有域名续费，约 55~70 元/年。GitHub Pages 托管和 HTTPS 证书永久免费。
