// Export des graphiques en PNG via le presse-papiers — demande explicite du
// 13/08/2026 : permettre à l'utilisateur de coller facilement une courbe
// dans un rapport (le téléchargement PNG a été retiré le 28/08/2026,
// demande explicite — la copie presse-papiers couvre le même besoin). Gère
// les deux types de rendu utilisés dans l'appli : <canvas> (Nomogramme
// 2D/3D) directement, <svg> (GraphiqueSVG, FiltreHampel) via une conversion
// (sérialisation -> <img> -> canvas hors-écran).
const FOND = "#0f1117"; // même couleur que --body-bg (index.css) : sans ça,
// un SVG exporté a un fond transparent, illisible une fois collé sur du blanc.

export function canvasVersDataUrl(canvas) {
  return canvas.toDataURL("image/png");
}

export function svgVersDataUrl(svgElement, echelle = 2) {
  return new Promise((resolve, reject) => {
    const clone = svgElement.cloneNode(true);
    const viewBox = svgElement.viewBox?.baseVal;
    const largeur = viewBox?.width || svgElement.clientWidth || 800;
    const hauteur = viewBox?.height || svgElement.clientHeight || 400;
    clone.setAttribute("width", largeur);
    clone.setAttribute("height", hauteur);
    clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");

    const fond = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    fond.setAttribute("width", "100%");
    fond.setAttribute("height", "100%");
    fond.setAttribute("fill", FOND);
    clone.insertBefore(fond, clone.firstChild);

    const source = new XMLSerializer().serializeToString(clone);
    const svgDataUrl = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(source)}`;

    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement("canvas");
      canvas.width = largeur * echelle;
      canvas.height = hauteur * echelle;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      resolve(canvas.toDataURL("image/png"));
    };
    img.onerror = () => reject(new Error("Conversion SVG -> image échouée"));
    img.src = svgDataUrl;
  });
}

export async function copierDataUrlDansPressePapiers(dataUrl) {
  const reponse = await fetch(dataUrl);
  const blob = await reponse.blob();
  await navigator.clipboard.write([new ClipboardItem({ [blob.type]: blob })]);
}
