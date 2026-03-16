PYTHON ?= python3
VERSION ?= 0.1.3
S3_BUCKET ?= gettelecode.com
CLOUDFRONT_ID ?= E3CT71G02GLRD5

release:
	$(PYTHON) - <<'PY'
	from pathlib import Path
	version = "$(VERSION)"
	path = Path("pyproject.toml")
	text = path.read_text()
	old = None
	for line in text.splitlines():
	    if line.startswith("version = "):
	        old = line
	        break
	if not old:
	    raise SystemExit("version not found in pyproject.toml")
	new = f'version = "{version}"'
	path.write_text(text.replace(old, new, 1))
	PY
	git add pyproject.toml
	git commit -m "Release v$(VERSION)"
	git tag v$(VERSION)
	git push origin main v$(VERSION)
	@echo "Released v$(VERSION)"

test:
	$(PYTHON) -m pytest -q

deploy:
	@echo "Deploying website to S3..."
	aws s3 sync docs/ s3://$(S3_BUCKET)/ --delete --exclude ".DS_Store"
	@echo "Creating CloudFront invalidation..."
	aws cloudfront create-invalidation --distribution-id $(CLOUDFRONT_ID) --paths "/*" --query 'Invalidation.{Id:Id,Status:Status,CreateTime:CreateTime}' --output table
	@echo ""
	@echo "✓ Website deployed successfully!"
	@echo "  S3 Bucket: s3://$(S3_BUCKET)"
	@echo "  CloudFront: https://$(S3_BUCKET)"
	@echo "  Cache invalidation in progress (2-5 minutes)"
