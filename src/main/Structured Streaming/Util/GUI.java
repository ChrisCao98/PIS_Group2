package Util;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class GUI extends JFrame {
    private JLabel imageLabel;

    public GUI() {
        setTitle("Image Viewer");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        imageLabel = new JLabel();
        imageLabel.setPreferredSize(new Dimension(1800, 1000));
        add(imageLabel, BorderLayout.CENTER);

        pack();
        setVisible(true);
    }
    public void displayImage(byte[] imageData) {
        SwingUtilities.invokeLater(() -> {
            try {
                // 将字节数组转换为图像
                ByteArrayInputStream inputStream = new ByteArrayInputStream(imageData);
                BufferedImage originalImage = ImageIO.read(inputStream);

                // 设置图像标签的图像
                imageLabel.setIcon(new ImageIcon(originalImage));

                // 调整图像大小以适应标签
                int labelWidth = imageLabel.getWidth();
                int labelHeight = imageLabel.getHeight();

                // 创建缩放后的图像
                BufferedImage scaledImage = new BufferedImage(labelWidth, labelHeight, BufferedImage.TYPE_INT_ARGB);
                Graphics2D g2d = scaledImage.createGraphics();
                g2d.drawImage(originalImage, 0, 0, labelWidth, labelHeight, null);
                g2d.dispose();

                // 设置图像标签的图像
                imageLabel.setIcon(new ImageIcon(scaledImage));

                pack();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    public static byte[] readImage(String filePath) {
        try {
            File imageFile = new File(filePath);
            BufferedImage bufferedImage = ImageIO.read(imageFile);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(bufferedImage, "jpg", outputStream);
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    public static void main(String[] args) {
        // 创建图形化界面
        GUI gui = new GUI();

        // 假设你有一个方法，从某处获取字节数组的图像数据
        byte[] imageData = readImage("/home/chriscao/IdeaProjects/kfaka_no_gui/src/main/resources/testImage/image.jpg");

        // 在图形化界面上显示图像
        gui.displayImage(imageData);
    }


}

