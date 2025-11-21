## Scalar Quantization and Metrics Calculation for Quantized Vectors

The quantization is implemented via min-max normalization. This method linearly scales each component of the vector based on the precomputed minimum and maximum of the original floating-point data:

$$i = \left\lfloor \frac{\mathbf{C}}{f_{\max} - f_{\min}} \cdot \left(\max(\min(f, f_{\max}), f_{\min}) - f_{\min}\right) \right\rfloor$$

**Where:**

$\begin{aligned}
\text{}\scriptsize\bullet\text{\ \ } & f         & - &\text{ original floating-point number}\\
\text{}\scriptsize\bullet\text{\ \ } & (f_{\min}, f_{\max})  & - &\text{ range of quantization}\\
\text{}\scriptsize\bullet\text{\ \ } & i         & - &\text{ quantized integer representation of } f\\
\text{}\scriptsize\bullet\text{\ \ } & \mathbf{C}& - &\text{ integer range of quantized values }(\mathbf{C} = 255 \text{ for 8-bit unsigned int})\\
\end{aligned}$

The restored dequantized floating-point value $f'$ is calculated as:

$$f' \approx \frac{f_{\max} - f_{\min}}{\mathbf{C}} \cdot i + f_{\min}$$

Thus, $f = f' + d$, where $d$ is the rounding error.

### The Case of the Euclidean Scalar Product

Let $(\vec{f}_1, \vec{f}_2)$ denote the Euclidean scalar product of two vectors $\vec{f}_1$ and $\vec{f}_2$. Then:

$$(\vec{f}_1, \vec{f}_2) = (\vec{f}'_1 + \vec{d}_1, \vec{f}'_2 + \vec{d}_2) = (\vec{f}'_1, \vec{f}'_2) + (\vec{f}_1, \vec{d}_2) + (\vec{d}_1, \vec{f}_2) + (\vec{d}_1, \vec{d}_2)$$

Introducing the notation $\alpha = \frac{f_{\max} - f_{\min}}{\mathbf{C}}$, and substituting the expressions for $f'$, we can write for the $j$-th components:

$$
\begin{aligned}
f_{1j} \cdot f_{2j} &= f'_{1j} \cdot f'_{2j} + f'_{1j} \cdot d_{2j} + f'_{2j} \cdot d_{1j} + d_{1j} \cdot d_{2j} = \\
&= \alpha^2 i_{1j} i_{2j} + \alpha f_{\min} (i_{1j} + i_{2j}) + f_{\min}^2 + (\alpha \cdot i_{1j} + f_{\min}) \cdot d_{2j} + (\alpha \cdot i_{2j} + f_{\min}) \cdot d_{1j} + d_{1j} \cdot d_{2j} = \\
&= \alpha^2 i_{1j} i_{2j} + \alpha f_{\min} (i_{1j} + i_{2j}) + f_{\min}^2 + f_{\min} (d_{1j} + d_{2j}) + \alpha (i_{1j} d_{2j} + i_{2j} d_{1j}) + d_{1j} \cdot d_{2j}
\end{aligned}
$$

Since $(\vec{f}_1, \vec{f}_2) = \sum_{j=1}^{\text{dim}} f_{1j} \cdot f_{2j}$,

$$
\begin{aligned}
(\vec{f}_1, \vec{f}_2) &= \alpha^2 \sum_{j=1}^{\text{dim}} i_{1j} i_{2j} + \alpha f_{\min} \sum_{j=1}^{\text{dim}} (i_{1j} + i_{2j}) + f_{\min}^2 \cdot \text{dim} +\\
&\quad + f_{\min} \sum_{j=1}^{\text{dim}} (d_{1j} + d_{2j}) + \alpha \sum_{j=1}^{\text{dim}} (i_{1j} d_{2j} + i_{2j} d_{1j}) + \sum_{j=1}^{\text{dim}} d_{1j} \cdot d_{2j}
\end{aligned}
$$

Thus, considering that:
* $\sum_{j=1}^{\text{dim}} i_{1j} i_{2j}$ is the scalar product of quantized vectors $\vec{i}_1$ and $\vec{i}_2$,
* $\sum_{j=1}^{\text{dim}} i_{1j}$ is the sum of elements (or trace) of vector $\vec{i}_1$ (and similarly for $\vec{i}_2$),
* the term $\sum_{j=1}^{\text{dim}} d_{1j} \cdot d_{2j}$ can be neglected as a second-order small value

we get:

$$
\begin{aligned}
(\vec{f}_1, \vec{f}_2) &= \alpha^2 (\vec{i}_1, \vec{i}_2) + \alpha f_{\min} \cdot \text{tr}(\vec{i}_1) + f_{\min} \cdot \text{tr}(\vec{d}_1) + \alpha f_{\min} \cdot \text{tr}(\vec{i}_2) + f_{\min} \cdot \text{tr}(\vec{d}_2) + f_{\min}^2 \cdot \text{dim} + \alpha \sum_{j=1}^{\text{dim}} (i_{1j} d_{2j} + i_{2j} d_{1j})
\end{aligned}
$$

The factors $\alpha^2$ and the terms $\left[\alpha f_{\min} \cdot \text{tr}(\vec{i}) + f_{\min} \cdot \text{tr}(\vec{d}) + \frac{1}{2} \cdot f_{\min}^2 \cdot \text{dim}\right]$ can be precomputed during quantization and stored as corrective offsets for each quantized vector ($corr_1, corr_2$):

$$(\vec{f}_1, \vec{f}_2) = \left[\alpha^2 (\vec{i}_1, \vec{i}_2) + corr_1 + corr_2\right] + \alpha \sum_{j=1}^{\text{dim}} (i_{1j} d_{2j} + i_{2j} d_{1j})$$

The additional nonlinear term $\alpha \sum_{j=1}^{\text{dim}} (i_{1j} d_{2j} + i_{2j} d_{1j})$ cannot be precomputed. Its contribution becomes significant only in the presence of outliers comparable in magnitude to the component values. By default, this term is disabled but can be enabled using the appropriate Sq8-config parameter. The final expression for the quantized metric calculation then takes the form:

$$(\vec{f}_1, \vec{f}_2) = \alpha^2 (\vec{i}_1, \vec{i}_2) + corr_1 + corr_2$$

#### Calculation of the Additional Nonlinear Term

Storing the entire error vector would defeat the purpose of quantization. Therefore, we approximate the rounding error for each vector. Problems arise when outliers are large compared to the component values. In this case, we calculate three average errors:
* The mean rounding error (${err}^\circ$) for values within the range $(f_{\min}, f_{\max})$,
* The mean outlier value below the minimum quantile (${err}^+$),
* The mean outlier value above the maximum quantile (${err}^-$).

Quantization is effectively performed on the interval [1, 254], reserving values 0 and 255 for corresponding outliers. This ensures that the mean rounding errors (not the outlier errors) are applied to values inside $(f_{\min}, f_{\max})$ projected to 0 and 255. Thus, the expression becomes:

$$
\alpha \sum_{j=1}^{\text{dim}} (i_{1j} d_{2j} + i_{2j} d_{1j}) = \alpha \sum_{j=1}^{\text{dim}} \left( i_{1j} \cdot \begin{cases}
err^-_2 & \text{if } i_{2j} = 0 \\
err^+_2 & \text{if } i_{2j} = 255 \\
err^\circ_2 & \text{otherwise}
\end{cases} + i_{2j} \cdot \begin{cases}
err^-_1 & \text{if } i_{1j} = 0 \\
err^+_1 & \text{if } i_{1j} = 255 \\
err^\circ_1 & \text{otherwise}
\end{cases} \right)
$$

### The Case of the L2 Metric

By definition, the squared L2 distance between two points is calculated as $\rho^2(\vec{f}_1, \vec{f}_2) = \sum_{j=1}^{\text{dim}} (f_{1j} - f_{2j})^2$. Using the dependencies derived earlier, we express this in terms of the quantized vector components:

$$
\rho^2(\vec{f}_1, \vec{f}_2) = \sum_{j=1}^{\text{dim}} (f_{1j} - f_{2j})^2 =\sum_{j=1}^{\text{dim}} \left((f'_{1j} + d_{1j}) - (f'_{2j} + d_{2j})\right)^2 =\\
=\sum_{j=1}^{\text{dim}} \left((\alpha \cdot i_{1j} + f_{\min}) - (\alpha \cdot i_{2j} + f_{\min}) + (d_{1j} - d_{2j})\right)^2 = \\
=\alpha^2 \sum_{j=1}^{\text{dim}} (i_{1j} - i_{2j})^2 + 2\alpha\sum_{j=1}^{\text{dim}} (i_{1j} - i_{2j})\cdot(d_{1j} - d_{2j}) +\sum_{j=1}^{\text{dim}}(d_{1j} - d_{2j})^2 = \\
=\alpha^2 \sum_{j=1}^{\text{dim}} (i_{1j} - i_{2j})^2 + \sum_{j=1}^{\text{dim}} ( 2\alpha\cdot i_{1j}d_{1j} + d^2_{1j} ) + \sum_{j=1}^{\text{dim}} ( 2\alpha\cdot i_{2j}d_{2j} + d^2_{2j} ) - 2\alpha\sum_{j=1}^{\text{dim}} ( i_{2j}d_{1j} + i_{1j}d_{2j}) - 2\sum_{j=1}^{\text{dim}}d_{1j}d_{2j}
$$

Thus, considering that:
* $\alpha^2 \sum_{j=1}^{\text{dim}} (i_{1j} - i_{2j})^2$ is the L2-distance $\alpha^2 \cdot \rho^2(\vec{i}_1, \vec{i}_2)$ between quantized vectors $\vec{i}_1$ and $\vec{i}_2$,
* $\sum_{j=1}^{\text{dim}} ( 2\alpha\cdot i_{kj}d_{kj} + d^2_{kj} )$ is the scalar products of vectors $2\alpha\cdot\vec{i}_k + \vec{d}_k \text {and } \vec{d}_k \text{, k = 1,2}$, which, as in the case of the Scalar Product metric, can be precomputed and stored as corrective offsets for each quantized vector ($corr_1, corr_2$),
* as in the case of the Scalar Product metric, we have the same non-precomputable nonlinear term $- 2\alpha\sum_{j=1}^{\text{dim}} ( i_{2j}d_{1j} + i_{1j}d_{2j})$. This term is also disabled by default,
* the term $-2\sum_{j=1}^{\text{dim}} d_{1j} \cdot d_{2j}$ can be neglected as a second-order small value

we get final expression for the quantized metric calculation:
$$
\rho^2(\vec{f}_1, \vec{f}_2) = \alpha^2 \cdot \rho^2(\vec{i}_1, \vec{i}_2) + (2\alpha\cdot\vec{i}_1 + \vec{d}_1, \vec{d}_1) + (2\alpha\cdot\vec{i}_2 + \vec{d}_2, \vec{d}_2) = \alpha^2 \cdot \rho^2(\vec{i}_1, \vec{i}_2) + corr_1 + corr_2 
$$