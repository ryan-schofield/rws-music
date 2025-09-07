# Music Tracker Deployment Cost Analysis

## Phase 1: AWS Lightsail (Internal-Only Deployment)

### Infrastructure Costs

| Component | Specification | Monthly Cost |
|-----------|---------------|--------------|
| **Lightsail Instance** | medium_2_0 (4GB RAM, 2 vCPUs, 80GB SSD) | $20.00 |
| **Static IP** | Included with instance | $0.00 |
| **Data Transfer** | 2TB included (sufficient for internal use) | $0.00 |
| **Route 53 DNS** | Optional - only if managing DNS in AWS | $0.50 |
| **Backup Storage** | ~5GB estimated storage needs | ~$1.00 |

### **Total Monthly Cost: $20.50 - $21.50**

### Cost Breakdown by Service
- **Compute**: $20/month (fixed)
- **Storage**: Included in instance cost
- **Networking**: Free for internal use
- **DNS**: $0.50/month (optional)
- **Backup**: ~$1/month (local + optional S3)

### Traffic and Storage Projections

**Current Data Profile:**
- DuckDB Database: ~250MB
- PostgreSQL Databases: ~100MB combined
- Docker Images: ~2GB
- Application Logs: ~50MB/month

**Annual Growth:**
- Data Growth: ~20MB/year (very minimal)
- Log Growth: ~600MB/year
- **5-Year Total**: Still well within 80GB SSD capacity

### Comparison with Original Microsoft Fabric

| Metric | Microsoft Fabric | AWS Lightsail | Savings |
|--------|-----------------|---------------|---------|
| **Monthly Cost** | $200-500 | $21 | **91-96%** |
| **Annual Cost** | $2,400-6,000 | $252 | **91-96%** |
| **Setup Complexity** | High | Low | ✅ |
| **Vendor Lock-in** | High | Moderate | ✅ |
| **Scaling Cost** | Linear/Exponential | Predictable | ✅ |

### Future Cost Optimization Path

**Phase 2: Hetzner Cloud** (Future)
- **Instance**: CPX21 (4GB RAM, 3 vCPUs, 80GB SSD) = $8/month
- **Total**: ~$8-10/month
- **Additional Savings**: 60% from Phase 1

**Phase 3: Oracle Free Tier** (Future)
- **Instance**: 4GB RAM, 2-4 vCPUs, 200GB storage = $0/month (permanent free tier)
- **Total**: $0/month
- **Risk**: Service changes, account termination policies

### Cost Scaling Scenarios

**If Data Grows 10x (Unlikely):**
- Upgrade to large_2_0 (8GB RAM, 2 vCPUs, 160GB SSD) = $40/month
- Still 85% savings vs original Fabric solution

**If Need High Availability:**
- Add second Lightsail instance = +$20/month
- Load Balancer = +$18/month
- Total: ~$58/month (still 75% savings)

### Recommendations

1. **Start with Phase 1** at $21/month to validate the solution
2. **Monitor usage** for 3-6 months to understand actual resource needs
3. **Migrate to Hetzner** (Phase 2) once comfortable with the setup
4. **Consider Oracle Free Tier** (Phase 3) for maximum cost optimization

### Break-Even Analysis

**Time to ROI from Fabric Migration:**
- Monthly Savings: $179-479 (using $200-500 Fabric cost)
- Implementation Time: 1-2 days
- **Payback Period: Immediate** (first month savings exceed setup effort)

**Annual Savings:**
- Conservative: $2,148/year (vs $200/month Fabric)
- Aggressive: $5,748/year (vs $500/month Fabric)