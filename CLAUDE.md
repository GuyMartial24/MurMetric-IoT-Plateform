# Frontend Design Standards

## UI Stack
- Framework: React / Next.js (App Router)
- Styling: Tailwind CSS (palette Slate/Zinc)
- Components: shadcn/ui, Radix UI primitives
- Icons: Lucide React (size: 16-20px, stroke: 1.5-2px)
- Animations: Framer Motion (subtle transitions only, max 300ms)

## Design Rules
1. Never use raw HTML buttons or inputs; use shadcn/ui components.
2. Structure: Maintain consistent spacing (p-4/p-6, gap-4/gap-6).
3. Feedback states: Always implement Loading (Skeleton), Empty states (Icon + Title + Action), and Error states.
4. Colors: Strict neutral background with one accent primary color; ensure WCAG AA contrast.
5. Typography: Strict hierarchical scale (text-xs for metadata, text-sm for body/tables, text-base/lg for headings).