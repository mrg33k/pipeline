# Live Test Results: AOM Cold Outreach Pipeline

**Date:** 2026-02-27  
**Target:** Hospitality / Restaurants in Scottsdale/Phoenix metro  
**Batch size:** 5 contacts

---

## Pipeline Summary

| Step | Description | Result |
|------|-------------|--------|
| 1. Apollo Search | 4 keyword searches (restaurant, hospitality, hotel, bar food dining) | **66 unique candidates found** |
| 2. LLM Filtering | gpt-4.1-mini picked 5 best hospitality/restaurant operators | **5 selected** |
| 3. Apollo Enrichment | Bulk enrichment for emails | **5/5 matched, 5 credits consumed** |
| 4. Company Research | Homepage scraping for context | **5/5 researched** |
| 5. Email Writing | Personalized emails via gpt-4.1-mini | **5/5 written** |
| 6. Gmail Drafts | Create drafts via Gmail API | **0/5 (Gmail API not enabled)** |
| 7. CSV Export | Full batch exported | **Saved to daily_exports/** |

---

## Contacts Enriched

| # | Name | Title | Company | Email | City |
|---|------|-------|---------|-------|------|
| 1 | Laurent Halasz | Founder | Francine Restaurant | laurent@francinerestaurant.com | Scottsdale, AZ |
| 2 | Cindy Huie | Owner | Salt Cellar Restaurant | cindy@saltcellarrestaurant.com | Scottsdale, AZ |
| 3 | Kyle Walters | Restaurant Owner | The Parlay Kitchen + Cocktails | kyle@theparlayaz.com | Chandler, AZ |
| 4 | Arsal Jaffery | CEO | Reliance Hospitality | ajaffery@mhgi.net | Phoenix, AZ |
| 5 | Kim Dansereau | Owner | Kind Hospitality | kim@feedhumankind.com | Mesa, AZ |

---

## Email Previews

### Email 1: Laurent Halasz at Francine Restaurant

**Subject:** video for Francine Restaurant  
**To:** laurent@francinerestaurant.com

> Laurent,
>
> Francine's blend of French Mediterranean flavors stands out in Scottsdale's dining scene. Your airy, elegant space looks like a perfect spot for a memorable meal.
>
> A short video showing the art behind your dishes, from kitchen to table, could connect with locals who seek a refined yet approachable experience. Highlighting your team's craft and passion could bring Francine's story to life on social channels.
>
> I'm based in Phoenix and know the Valley's appetite for quality dining. If you want to explore the idea, I'm happy to chat.
>
> Cheers,

---

### Email 2: Cindy Huie at Salt Cellar Restaurant

**Subject:** video for Salt Cellar Restaurant  
**To:** cindy@saltcellarrestaurant.com

> Cindy,
>
> Salt Cellar's concept of an underground seafood dining experience stands out in Scottsdale. Your focus on fresh Arizona seafood deserves a story that matches its uniqueness.
>
> A short video showing the journey of your seafood from local waters to the table could connect with your audience. Highlight the atmosphere beneath the city and the care in every dish.
>
> Being nearby in Phoenix, I understand the local dining scene and what draws people to experiences like yours. I'd like to discuss how a simple, authentic video can bring more diners through your doors.
>
> If you're open to it, let's set up a quick call.
>
> Cheers,

---

### Email 3: Kyle Walters at The Parlay Kitchen + Cocktails

**Subject:** video for The Parlay Kitchen + Cocktails  
**To:** kyle@theparlayaz.com

> Kyle,
>
> The Parlay Kitchen + Cocktails has a strong local vibe that fits Chandler's community feel. Your menu strikes a solid balance between comfort food and creative cocktails.
>
> A quick video showing a day behind the scenes could highlight your team's energy and your kitchen's craft. It could also capture the neighborhood atmosphere that keeps customers coming back.
>
> We're just over in Phoenix and understand the local food scene well. If you want to explore how this could look for The Parlay, I'm happy to chat.
>
> Cheers,

---

### Email 4: Arsal Jaffery at Reliance Hospitality

**Subject:** video for Reliance Hospitality  
**To:** ajaffery@mhgi.net

> Arsal,
>
> Reliance Hospitality's focus on boosting hotel revenue stands out. I have an idea for a video that highlights how your sales solutions turn missed opportunities into bookings. The video could follow a hotel manager using your approach to fill rooms and grow profits.
>
> Being in Phoenix means you know the local hospitality market well. I can create a short video that connects with nearby hotels and travel businesses.
>
> Let me know if you want to talk about how this could fit with your marketing.
>
> Cheers,

---

### Email 5: Kim Dansereau at Kind Hospitality

**Subject:** video for Kind Hospitality  
**To:** kim@feedhumankind.com

> Kim,
>
> Your focus on service from the heart stands out. I see a video showing the faces behind Kind Hospitality, sharing what makes your team special, would fit well on your website and social channels.
>
> Mesa has a close-knit food scene. A short video highlighting your local roots and community connections could resonate with your customers.
>
> If you want to talk about how this might work for Kind Hospitality, I'm nearby and happy to meet.
>
> Cheers,

---

## Gmail Draft Issue

All 5 drafts failed with the same error:

```
Gmail API has not been used in project 618386231675 before or it is disabled.
Enable it by visiting:
https://console.developers.google.com/apis/api/gmail.googleapis.com/overview?project=618386231675
```

**To fix this:** Go to the Google Cloud Console link above and click "Enable API" for the Gmail API on project 618386231675. After enabling, wait a few minutes, then re-run the pipeline. Everything else (Apollo search, enrichment, research, email writing) works perfectly.

---

## API Credit Usage

| Service | Credits Used | Notes |
|---------|-------------|-------|
| Apollo Search | 0 | People Search is free |
| Apollo Enrichment | 5 | 1 credit per person |
| OpenAI (gpt-4.1-mini) | ~2 calls | 1 for LLM filtering, 5 for email writing |

---

## Files Generated

- `daily_exports/live_test_2026-02-27_*.csv` - Full batch with all contact details and email bodies
- `live_test.py` - The live test script
- `live_test_results.md` - This report
