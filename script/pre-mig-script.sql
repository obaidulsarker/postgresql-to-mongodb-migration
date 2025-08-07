UPDATE "product"."ProductLogs" SET
	"CreatedAt" = "UpdatedAt"
	WHERE "CreatedAt" IS NULL;

	UPDATE "product"."ProductLogs" SET
	"CreatedAt" = '2024-08-01 00:00:00+00'::timestamp with time zone
	WHERE "CreatedAt" IS NULL;

	UPDATE "product"."ProductVariantLogs" SET
	"CreatedAt" = "UpdatedAt"
	WHERE "CreatedAt" IS NULL;

	UPDATE "product"."ProductVariantLogs" SET
	"CreatedAt" = '2024-08-01 00:00:00+00'::timestamp with time zone
	WHERE "CreatedAt" IS NULL;